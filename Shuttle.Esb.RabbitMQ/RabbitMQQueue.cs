using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQQueue : IQueue, ICreateQueue, IDropQueue, IDisposable, IPurgeQueue
    {
        private readonly Dictionary<string, object> _arguments = new Dictionary<string, object>();
        private readonly CancellationToken _cancellationToken;

        private readonly ConnectionFactory _factory;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private readonly int _operationRetryCount;
        private readonly RabbitMQOptions _rabbitMQOptions;
        private Channel _channel;

        private IConnection _connection;

        private bool _disposed;

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued = delegate
        {
        };

        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged = delegate
        {
        };

        public event EventHandler<MessageReleasedEventArgs> MessageReleased = delegate
        {
        };

        public event EventHandler<MessageReceivedEventArgs> MessageReceived = delegate
        {
        };

        public event EventHandler<OperationCompletedEventArgs> OperationCompleted = delegate
        {
        };

        public RabbitMQQueue(QueueUri uri, RabbitMQOptions rabbitMQOptions, CancellationToken cancellationToken)
        {
            Uri = Guard.AgainstNull(uri, nameof(uri));
            _rabbitMQOptions = Guard.AgainstNull(rabbitMQOptions, nameof(rabbitMQOptions));
            _cancellationToken = cancellationToken;

            if (_rabbitMQOptions.Priority != 0)
            {
                _arguments.Add("x-max-priority", _rabbitMQOptions.Priority);
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

        public async Task Create()
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() => QueueDeclare(GetChannel().Model));

                OperationCompleted.Invoke(this, new OperationCompletedEventArgs("Create"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Dispose()
        {
            _lock.Wait(CancellationToken.None);

            try
            {
                _channel?.Dispose();

                _disposed = true;

                CloseConnection();
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Drop()
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() =>
                {
                    GetChannel().Model.QueueDelete(Uri.QueueName);
                });

                OperationCompleted.Invoke(this, new OperationCompletedEventArgs("Drop"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Purge()
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            
            try
            {
                AccessQueue(() =>
                {
                    GetChannel().Model.QueuePurge(Uri.QueueName);
                });

                OperationCompleted.Invoke(this, new OperationCompletedEventArgs("Purge"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public QueueUri Uri { get; }
        public bool IsStream => false;

        public async ValueTask<bool> IsEmpty()
        {
            if (_disposed)
            {
                return await new ValueTask<bool>(true);
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                return await new ValueTask<bool>(AccessQueue(() =>
                {
                    var result = GetChannel().Model.BasicGet(Uri.QueueName, false);

                    if (result == null)
                    {
                        return true;
                    }

                    GetChannel().Model.BasicReject(result.DeliveryTag, true);

                    return false;
                }));
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Enqueue(TransportMessage transportMessage, Stream stream)
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

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() =>
                {
                    var model = GetChannel().Model;

                    var properties = model.CreateBasicProperties();

                    properties.Persistent = _rabbitMQOptions.Persistent;
                    properties.CorrelationId = transportMessage.MessageId.ToString();

                    if (transportMessage.HasExpiryDate())
                    {
                        var milliseconds = (long)(transportMessage.ExpiryDate - DateTime.Now).TotalMilliseconds;

                        if (milliseconds < 1)
                        {
                            milliseconds = 1;
                        }

                        properties.Expiration = milliseconds.ToString();
                    }

                    if (transportMessage.HasPriority())
                    {
                        if (transportMessage.Priority > 255)
                        {
                            transportMessage.Priority = 255;
                        }

                        properties.Priority = (byte)transportMessage.Priority;
                    }

                    ReadOnlyMemory<byte> data;

                    if (stream is MemoryStream ms && ms.TryGetBuffer(out var segment))
                    {
                        var length = (int)ms.Length;
                        data = new ReadOnlyMemory<byte>(segment.Array, segment.Offset, length);
                    }
                    else
                    {
                        data = stream.ToBytes();
                    }

                    model.BasicPublish(string.Empty, Uri.QueueName, false, properties, data);
                });

                MessageEnqueued.Invoke(this, new MessageEnqueuedEventArgs(transportMessage, stream));
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task<ReceivedMessage> GetMessage()
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                return await Task.FromResult(AccessQueue(() =>
                {
                    var result = GetChannel().Next();

                    var receivedMessage = result == null
                        ? null
                        : new ReceivedMessage(new MemoryStream(result.Data, 0, result.DataLength, false, true), result);

                    if (receivedMessage != null)
                    {
                        MessageReceived.Invoke(this, new MessageReceivedEventArgs(receivedMessage));
                    }

                    return receivedMessage;
                }));
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Acknowledge(object acknowledgementToken)
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                if (acknowledgementToken != null)
                {
                    AccessQueue(() => GetChannel().Acknowledge((DeliveredMessage)acknowledgementToken));

                    MessageAcknowledged.Invoke(this, new MessageAcknowledgedEventArgs(acknowledgementToken));
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Release(object acknowledgementToken)
        {
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() =>
                {
                    var token = (DeliveredMessage)acknowledgementToken;

                    var channel = GetChannel();
                    channel.Model.BasicPublish(string.Empty, Uri.QueueName, false, token.BasicProperties, token.Data.AsMemory(0, token.DataLength));
                    channel.Acknowledge(token);
                });

                MessageReleased.Invoke(this, new MessageReleasedEventArgs(acknowledgementToken));
            }
            finally
            {
                _lock.Release();
            }
        }

        private void AccessQueue(Action action, int retry = 0)
        {
            if (_disposed || _cancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                action.Invoke();
            }
            catch (ConnectionException)
            {
                if (retry == _operationRetryCount)
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
                return default;
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

        private void CloseConnection()
        {
            if (_connection == null)
            {
                return;
            }

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
        }

        private Channel GetChannel()
        {
            if (_connection != null && _channel != null && _channel.Model.IsOpen)
            {
                return _channel;
            }

            _channel?.Dispose();

            var retry = 0;
            _connection = null;

            while (_connection == null && retry < _operationRetryCount)
            {
                try
                {
                    _connection = GetConnection();
                }
                catch
                {
                    retry++;
                }
            }

            if (_connection == null)
            {
                throw new ConnectionException(string.Format(Resources.ConnectionException, Uri));
            }

            var model = _connection.CreateModel();

            model.BasicQos(0, _rabbitMQOptions.PrefetchCount, false);

            QueueDeclare(model);

            _channel = new Channel(model, Uri, _rabbitMQOptions);

            return _channel;
        }

        private IConnection GetConnection()
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection != null)
            {
                if (_connection.IsOpen)
                {
                    return _connection;
                }

                CloseConnection();
            }

            return _factory.CreateConnection(Uri.QueueName);
        }

        private void QueueDeclare(IModel model)
        {
            model.QueueDeclare(Uri.QueueName, _rabbitMQOptions.Durable, false, false, _arguments);

            try
            {
                model.QueueDeclarePassive(Uri.QueueName);

                OperationCompleted.Invoke(this, new OperationCompletedEventArgs("QueueDeclare"));
            }
            catch
            {
                OperationCompleted.Invoke(this, new OperationCompletedEventArgs("QueueDeclare (FAILED)"));
            }
        }
    }

    internal class DeliveredMessage
    {
        public IBasicProperties BasicProperties { get; set; }
        public byte[] Data { get; set; }

        public int DataLength { get; set; }

        public ulong DeliveryTag { get; set; }
    }
}