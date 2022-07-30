using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using RabbitMQ.Client;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQQueue : IQueue, ICreateQueue, IDropQueue, IDisposable, IPurgeQueue
    {
        private static readonly object ChannelLock = new object();

        [ThreadStatic] private static ConditionalWeakTable<IConnection, Channel> _threadChannels;

        private readonly Dictionary<string, object> _arguments = new Dictionary<string, object>();
        private readonly Dictionary<Channel, WeakReference<Thread>> _channels = new Dictionary<Channel, WeakReference<Thread>>();
        private readonly List<Channel> _channelsToRemove = new List<Channel>();
        private readonly RabbitMQOptions _rabbitMQOptions;
        
        private readonly ConnectionFactory _factory;

        private readonly int _operationRetryCount;

        private volatile IConnection _connection;
        private bool _disposed;

        public RabbitMQQueue(QueueUri uri, RabbitMQOptions rabbitMQOptions)
        {
            Guard.AgainstNull(uri, nameof(uri));
            Guard.AgainstNull(rabbitMQOptions, nameof(rabbitMQOptions));

            Uri = uri;

            _rabbitMQOptions = rabbitMQOptions;

            if (_rabbitMQOptions.Priority != 0)
            {
                _arguments.Add("x-max-priority", (int)_rabbitMQOptions.Priority);
            }

            _operationRetryCount = _rabbitMQOptions.OperationRetryCount;

            if (_operationRetryCount < 1)
            {
                _operationRetryCount = 3;
            }

            _factory = new ConnectionFactory
            {
                DispatchConsumersAsync = false,
                UserName = _rabbitMQOptions.Username,
                Password = _rabbitMQOptions.Password,
                HostName = _rabbitMQOptions.Host,
                VirtualHost = _rabbitMQOptions.VirtualHost,
                Port = _rabbitMQOptions.Port,
                RequestedHeartbeat = rabbitMQOptions.RequestedHeartbeat,
                UseBackgroundThreadsForIO = rabbitMQOptions.UseBackgroundThreadsForIO
            };
        }

        public void Create()
        {
            AccessQueue(() => QueueDeclare(GetChannel().Model));
        }

        public void Dispose()
        {
            CloseConnection();
            _disposed = true;
        }

        private void CloseConnection()
        {
            lock (ChannelLock)
            {
                foreach (var channel in _channels.Keys)
                {
                    channel.Dispose();
                }

                _channels.Clear();

                if (_connection != null)
                {
                    if (_connection.IsOpen)
                    {
                        _connection.Close(_rabbitMQOptions.ConnectionCloseTimeout);
                    }

                    try
                    {
                        _connection.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }

                    _connection = null;
                }
            }
        }

        public void Drop()
        {
            AccessQueue(() => { GetChannel().Model.QueueDelete(Uri.QueueName); });
        }

        public void Purge()
        {
            AccessQueue(() => { GetChannel().Model.QueuePurge(Uri.QueueName); });
        }

        public QueueUri Uri { get; }
        public bool IsStream => false;

        public bool IsEmpty()
        {
            if (_disposed)
            {
                return true;
            }

            return AccessQueue(() =>
            {
                var result = GetChannel().Model.BasicGet(Uri.QueueName, false);

                if (result == null)
                {
                    return true;
                }

                GetChannel().Model.BasicReject(result.DeliveryTag, true);

                return false;
            });
        }

        public void Enqueue(TransportMessage transportMessage, Stream stream)
        {
            Guard.AgainstNull(transportMessage, nameof(transportMessage));
            Guard.AgainstNull(stream, nameof(stream));

            if (_disposed)
            {
                throw new RabbitMQQueueException(string.Format(Resources.QueueDisposed, Uri));
            }

            if (transportMessage.HasExpired())
            {
                return;
            }

            AccessQueue(() =>
            {
                var model = GetChannel().Model;

                var properties = model.CreateBasicProperties();

                properties.Persistent = _rabbitMQOptions.Persistent;
                properties.CorrelationId = transportMessage.MessageId.ToString();

                if (transportMessage.HasExpiryDate())
                {
                    var milliseconds = (long) (transportMessage.ExpiryDate - DateTime.Now).TotalMilliseconds;

                    if (milliseconds < 1)
                    {
                        return;
                    }

                    properties.Expiration = milliseconds.ToString();
                }

                if (transportMessage.HasPriority())
                {
                    if (transportMessage.Priority > 255)
                    {
                        transportMessage.Priority = 255;
                    }

                    properties.Priority = (byte) transportMessage.Priority;
                }

                ReadOnlyMemory<byte> data;

                if (stream is MemoryStream ms && ms.TryGetBuffer(out var segment))
                {
                    var length = (int) ms.Length;
                    data = new ReadOnlyMemory<byte>(segment.Array, segment.Offset, length);
                }
                else
                {
                    data = stream.ToBytes();
                }

                model.BasicPublish(string.Empty, Uri.QueueName, false, properties, data);
            });
        }

        public ReceivedMessage GetMessage()
        {
            return AccessQueue(() =>
            {
                var result = GetChannel().Next();

                return result == null 
                    ? null 
                    : new ReceivedMessage(new MemoryStream(result.Data, 0, result.DataLength, false, true), result);
            });
        }

        public void Acknowledge(object acknowledgementToken)
        {
            if (acknowledgementToken == null)
            {
                return;
            }

            AccessQueue(() => GetChannel().Acknowledge((DeliveredMessage) acknowledgementToken));
        }

        public void Release(object acknowledgementToken)
        {
            AccessQueue(() =>
            {
                var token = (DeliveredMessage) acknowledgementToken;

                var channel = GetChannel();
                channel.Model.BasicPublish(string.Empty, Uri.QueueName, false, token.BasicProperties, token.Data.AsMemory(0, token.DataLength));
                channel.Acknowledge(token);
            });
        }

        private void QueueDeclare(IModel model)
        {
            model.QueueDeclare(Uri.QueueName, _rabbitMQOptions.Durable, false, false, _arguments);
        }

        private IConnection GetConnection()
        {
            if (_connection != null && _connection.IsOpen)
            {
                return _connection;
            }

            lock (ChannelLock)
            {
                if (_connection != null)
                {
                    if (_connection.IsOpen)
                    {
                        return _connection;
                    }

                    CloseConnection();
                }

                _connection = _factory.CreateConnection(Uri.QueueName);
            }

            return _connection;
        }

        private Channel GetChannel()
        {
            if (_threadChannels == null)
            {
                _threadChannels = new ConditionalWeakTable<IConnection, Channel>();
            }

            var connection = _connection;
            Channel channel = null;

            if (connection != null && _threadChannels.TryGetValue(connection, out channel) && channel.Model.IsOpen)
            {
                return channel;
            }

            if (channel != null)
            {
                _threadChannels.Remove(connection);
                channel.Dispose();
            }
            
            var retry = 0;
            connection = null;

            while (connection == null && retry < _operationRetryCount)
            {
                try
                {
                    connection = GetConnection();
                }
                catch 
                {
                    retry++;
                }
            }

            if (connection == null)
            {
                throw new ConnectionException(string.Format(Resources.ConnectionException, Uri));
            }

            var model = connection.CreateModel();

            model.BasicQos(0, _rabbitMQOptions.PrefetchCount, false);

            QueueDeclare(model);

            channel = new Channel(model, Uri, _rabbitMQOptions);

            _threadChannels.Add(connection, channel);

            lock (ChannelLock)
            {
                _channels.Add(channel, new WeakReference<Thread>(Thread.CurrentThread));

                var channelsToRemove = _channelsToRemove;

                foreach (var pair in _channels)
                {
                    if (!pair.Value.TryGetTarget(out _))
                    {
                        channelsToRemove.Add(pair.Key);
                    }
                }

                foreach (var channelToRemove in channelsToRemove)
                {
                    _channels.Remove(channelToRemove);
                    channelToRemove.Dispose();
                }

                _channelsToRemove.Clear();
            }

            return channel;
        }

        private void AccessQueue(Action action, int retry = 0)
        {
            if (_disposed)
            {
                return;
            }

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

                CloseConnection();

                AccessQueue(action, retry + 1);
            }
        }

        private T AccessQueue<T>(Func<T> action, int retry = 0)
        {
            if (_disposed)
            {
                return default (T);
            }

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

                CloseConnection();

                return AccessQueue(action, retry + 1);
            }
        }
    }

    internal class DeliveredMessage
    {
        public byte[]  Data { get; set; }

        public int DataLength { get; set; }
        
        public ulong DeliveryTag { get; set; }
        
        public IBasicProperties BasicProperties { get; set; }
    }
}