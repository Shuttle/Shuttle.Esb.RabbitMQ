using System;
using System.Collections.Generic;
using System.IO;
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

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued;
        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged;
        public event EventHandler<MessageReleasedEventArgs> MessageReleased;
        public event EventHandler<MessageReceivedEventArgs> MessageReceived;
        public event EventHandler<OperationEventArgs> Operation;

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
                RequestedHeartbeat = rabbitMQOptions.RequestedHeartbeat
            };

            _rabbitMQOptions.OnConfigureConsumer(this, new ConfigureEventArgs(_factory));
        }

        public void Create()
        {
            CreateAsync().GetAwaiter().GetResult();
        }

        public async Task CreateAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[create/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[create/starting]"));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() => QueueDeclare(GetChannel().Model));
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[create/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }

            Operation?.Invoke(this, new OperationEventArgs("[create/completed]"));
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

        public void Drop()
        {
            DropAsync().GetAwaiter().GetResult();
        }

        public async Task DropAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[drop/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[drop/starting]"));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                AccessQueue(() =>
                {
                    GetChannel().Model.QueueDelete(Uri.QueueName);
                });
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[drop/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }

            Operation?.Invoke(this, new OperationEventArgs("[drop/completed]"));
        }

        public void Purge()
        {
            PurgeAsync().GetAwaiter().GetResult();
        }

        public async Task PurgeAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[purge/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/starting]"));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            
            try
            {
                AccessQueue(() =>
                {
                    GetChannel().Model.QueuePurge(Uri.QueueName);
                });
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[purge/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/completed]"));
        }

        public QueueUri Uri { get; }
        public bool IsStream => false;

        public bool IsEmpty()
        {
            return IsEmptyAsync().GetAwaiter().GetResult();
        }

        public async ValueTask<bool> IsEmptyAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[is-empty/cancelled]", true));
                return true;
            }

            Operation?.Invoke(this, new OperationEventArgs("[is-empty/starting]"));

            if (_disposed)
            {
                return true;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                return await new ValueTask<bool>(AccessQueue(() =>
                {
                    var result = GetChannel().Model.BasicGet(Uri.QueueName, false);

                    var isEmpty = result == null;

                    if (!isEmpty)
                    {
                        GetChannel().Model.BasicReject(result.DeliveryTag, true);
                    }

                    Operation?.Invoke(this, new OperationEventArgs("[is-empty]", isEmpty));

                    return isEmpty;
                }));
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Enqueue(TransportMessage transportMessage, Stream stream)
        {
            EnqueueAsync(transportMessage, stream).GetAwaiter().GetResult();
        }

        public async Task EnqueueAsync(TransportMessage transportMessage, Stream stream)
        {
            Guard.AgainstNull(transportMessage, nameof(transportMessage));
            Guard.AgainstNull(stream, nameof(stream));

            if (_disposed)
            {
                throw new RabbitMQQueueException(string.Format(Resources.QueueDisposed, Uri));
            }

            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[enqueue/cancelled]"));
                return;
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

                MessageEnqueued?.Invoke(this, new MessageEnqueuedEventArgs(transportMessage, stream));
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[enqueue/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public ReceivedMessage GetMessage()
        {
            return GetMessageAsync().GetAwaiter().GetResult();
        }

        public async Task<ReceivedMessage> GetMessageAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[get-message/cancelled]"));
                return null;
            }

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
                        MessageReceived?.Invoke(this, new MessageReceivedEventArgs(receivedMessage));
                    }

                    return receivedMessage;
                }));
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[get-message/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }

            return null;
        }

        public void Acknowledge(object acknowledgementToken)
        {
            AcknowledgeAsync(acknowledgementToken).GetAwaiter().GetResult();
        }

        public async Task AcknowledgeAsync(object acknowledgementToken)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[acknowledge/cancelled]"));
                return;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                if (acknowledgementToken != null)
                {
                    AccessQueue(() => GetChannel().Acknowledge((DeliveredMessage)acknowledgementToken));

                    MessageAcknowledged?.Invoke(this, new MessageAcknowledgedEventArgs(acknowledgementToken));
                }
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[acknowledge/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Release(object acknowledgementToken)
        {
            ReleaseAsync(acknowledgementToken).GetAwaiter().GetResult();
        }

        public async Task ReleaseAsync(object acknowledgementToken)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[release/cancelled]"));
                return;
            }

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

                MessageReleased?.Invoke(this, new MessageReleasedEventArgs(acknowledgementToken));
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[release/cancelled]"));
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
            Operation?.Invoke(this, new OperationEventArgs("[queue-declare/starting]"));

            model.QueueDeclare(Uri.QueueName, _rabbitMQOptions.Durable, false, false, _arguments);

            try
            {
                model.QueueDeclarePassive(Uri.QueueName);

                Operation?.Invoke(this, new OperationEventArgs("[queue-declare/]"));
            }
            catch
            {
                Operation?.Invoke(this, new OperationEventArgs("[queue-declare/failed]"));
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