using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.RabbitMQ
{
	public class RabbitMQQueue : IQueue, ICreateQueue, IDropQueue, IDisposable, IPurgeQueue
	{
		private readonly IRabbitMQConfiguration _configuration;

		private static readonly object _connectionLock = new object();
		private static readonly object _queueLock = new object();
		private static readonly object _disposeLock = new object();

		private readonly int _operationRetryCount;

		private readonly RabbitMQUriParser _parser;

		private readonly ConnectionFactory _factory;
		private volatile IConnection _connection;

		private readonly Dictionary<int, Channel> _channels = new Dictionary<int, Channel>();

		public RabbitMQQueue(Uri uri, IRabbitMQConfiguration configuration)
		{
			Guard.AgainstNull(uri, "uri");
			Guard.AgainstNull(configuration, "configuration");

			_parser = new RabbitMQUriParser(uri);

			Uri = _parser.Uri;

			_configuration = configuration;

			_operationRetryCount = _configuration.OperationRetryCount;

			if (_operationRetryCount < 1)
			{
				_operationRetryCount = 3;
			}

			_factory = new ConnectionFactory
			{
				UserName = _parser.Username,
				Password = _parser.Password,
				HostName = _parser.Host,
				VirtualHost = _parser.VirtualHost,
				Port = _parser.Port,
				RequestedHeartbeat = (ushort) configuration.RequestedHeartbeat
			};
		}

		public Uri Uri { get; private set; }

		public bool IsEmpty()
		{
			return AccessQueue(() =>
			{
				var result = GetChannel().Model.BasicGet(_parser.Queue, false);

				if (result == null)
				{
					return true;
				}

				GetChannel().Model.BasicReject(result.DeliveryTag, true);

				return false;
			});
		}

		public bool HasUserInfo
		{
			get { return !string.IsNullOrEmpty(_parser.Username) && !string.IsNullOrEmpty(_parser.Password); }
		}

		public void Enqueue(TransportMessage transportMessage, Stream stream)
		{
			Guard.AgainstNull(transportMessage, "transportMessage");
			Guard.AgainstNull(stream, "stream");

			if (transportMessage.HasExpired())
			{
				return;
			}

			AccessQueue(() =>
			{
				var model = GetChannel().Model;

				var properties = model.CreateBasicProperties();

				properties.Persistent = _parser.Persistent;
				properties.CorrelationId = transportMessage.MessageId.ToString();

				if (transportMessage.HasExpiryDate())
				{
					properties.Expiration = (transportMessage.ExpiryDate - DateTime.Now).Milliseconds.ToString();
				}

				model.BasicPublish("", _parser.Queue, false, properties, stream.ToBytes());
			});
		}

		public ReceivedMessage GetMessage()
		{
			return AccessQueue(() =>
			{
				var result = GetChannel().Next();

				if (result == null)
				{
					return null;
				}

				return new ReceivedMessage(new MemoryStream(result.Body), result);
			});
		}

		public void Drop()
		{
			AccessQueue(() => { GetChannel().Model.QueueDelete(_parser.Queue); });
		}

		public void Create()
		{
			AccessQueue(() => QueueDeclare(GetChannel().Model));
		}

		private void QueueDeclare(IModel model)
		{
			model.QueueDeclare(_parser.Queue, _parser.Durable, false, false, null);
		}

		private IConnection GetConnection()
		{
			if (_connection != null && _connection.IsOpen)
			{
				return _connection;
			}

			lock (_connectionLock)
			{
				if (_connection != null && !_connection.IsOpen)
				{
					try
					{
						_connection.Dispose();
					}
					catch (Exception)
					{
					}

					_connection = null;
				}

				if (_connection == null)
				{
					_connection = _factory.CreateConnection();
				}

				_connection.AutoClose = false;
			}

			return _connection;
		}

		private Channel GetChannel()
		{
			var key = Thread.CurrentThread.ManagedThreadId;

			var channel = FindChannel(key);

			if (channel != null)
			{
				return channel;
			}

			lock (_queueLock)
			{
				channel = FindChannel(key);

				if (channel != null)
				{
					return channel;
				}

				var retry = 0;
				IConnection connection = null;

				while (connection == null && retry < _operationRetryCount)
				{
					try
					{
						connection = GetConnection();
					}
					catch (Exception)
					{
						retry++;
					}
				}

				if (connection == null)
				{
					throw new ConnectionException(string.Format(RabbitMQResources.ConnectionException, Uri.Secured()));
				}

				var model = connection.CreateModel();

				model.BasicQos(0,
					(ushort) (_parser.PrefetchCount == 0 ? _configuration.DefaultPrefetchCount : _parser.PrefetchCount), false);

				QueueDeclare(model);

				channel = new Channel(model, _parser, _configuration);

				_channels.Add(key, channel);

				return channel;
			}
		}

		private Channel FindChannel(int key)
		{
			Channel channel = null;

			if (_channels.ContainsKey(key))
			{
				channel = _channels[key];

				if (!channel.Model.IsOpen)
				{
					try
					{
						channel.Dispose();
					}
					catch (Exception)
					{
					}

					_channels.Remove(key);
					channel = null;
				}
			}

			return channel;
		}

		public void Dispose()
		{
			lock (_disposeLock)
			{
				foreach (var value in _channels.Values)
				{
					if (value.Model != null)
					{
						if (value.Model.IsOpen)
						{
							value.Model.Close();
						}

						try
						{
							value.Model.Dispose();
						}
						catch (Exception)
						{
						}
					}

					try
					{
						value.Dispose();
					}
					catch (Exception)
					{
					}
				}

				_channels.Clear();

				if (_connection != null)
				{
					if (_connection.IsOpen)
					{
						_connection.Close(_configuration.ConnectionCloseTimeoutMilliseconds);
					}

					try
					{
						_connection.Dispose();
					}
					catch (Exception)
					{
					}

					_connection = null;
				}
			}
		}

		public void Acknowledge(object acknowledgementToken)
		{
			AccessQueue(() => GetChannel().Acknowledge((BasicDeliverEventArgs) acknowledgementToken));
		}

		public void Release(object acknowledgementToken)
		{
			AccessQueue(() =>
			{
				var basicDeliverEventArgs = (BasicDeliverEventArgs) acknowledgementToken;

				GetChannel()
					.Model.BasicPublish("", _parser.Queue, false, basicDeliverEventArgs.BasicProperties, basicDeliverEventArgs.Body);
				GetChannel().Acknowledge(basicDeliverEventArgs);
			});
		}

		public void Purge()
		{
			AccessQueue(() => { GetChannel().Model.QueuePurge(_parser.Queue); });
		}

		private void AccessQueue(Action action, int retry = 0)
		{
			try
			{
				action.Invoke();
			}
			catch (ConnectionException)
			{
				if (retry == 3)
				{
					throw;
				}

				Dispose();

				AccessQueue(action, retry + 1);
			}
		}

		private T AccessQueue<T>(Func<T> action, int retry = 0)
		{
			try
			{
				return action.Invoke();
			}
			catch (ConnectionException)
			{
				if (retry == 3)
				{
					throw;
				}

				Dispose();

				return AccessQueue(action, retry + 1);
			}
		}
	}
}