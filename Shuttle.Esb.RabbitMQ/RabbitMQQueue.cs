using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.RabbitMQ;

public class RabbitMQQueue : IQueue, ICreateQueue, IDropQueue, IPurgeQueue, IDisposable
{
    private readonly Dictionary<string, object> _arguments = new();
    private readonly CancellationToken _cancellationToken;

    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private readonly int _operationRetryCount;
    private readonly RabbitMQOptions _rabbitMQOptions;

    private Channel? _channel;
    private IConnection? _connection;

    private bool _disposed;

    public RabbitMQQueue(QueueUri uri, RabbitMQOptions rabbitMQOptions, CancellationToken cancellationToken)
    {
        Uri = Guard.AgainstNull(uri);
        _rabbitMQOptions = Guard.AgainstNull(rabbitMQOptions);
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

        _factory = new()
        {
            DispatchConsumersAsync = false,
            UserName = _rabbitMQOptions.Username,
            Password = _rabbitMQOptions.Password,
            HostName = _rabbitMQOptions.Host,
            VirtualHost = _rabbitMQOptions.VirtualHost,
            Port = _rabbitMQOptions.Port,
            RequestedHeartbeat = rabbitMQOptions.RequestedHeartbeat
        };

        _rabbitMQOptions.OnConfigureConsumer(this, new(_factory));
    }

    public async Task CreateAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[create/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[create/starting]"));

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            AccessQueue(() => QueueDeclare(GetChannel().Model));
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[create/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }

        Operation?.Invoke(this, new("[create/completed]"));
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

    public async Task DropAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[drop/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[drop/starting]"));

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
            Operation?.Invoke(this, new("[drop/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }

        Operation?.Invoke(this, new("[drop/completed]"));
    }

    public async Task PurgeAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[purge/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[purge/starting]"));

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
            Operation?.Invoke(this, new("[purge/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }

        Operation?.Invoke(this, new("[purge/completed]"));
    }

    public event EventHandler<MessageEnqueuedEventArgs>? MessageEnqueued;
    public event EventHandler<MessageAcknowledgedEventArgs>? MessageAcknowledged;
    public event EventHandler<MessageReleasedEventArgs>? MessageReleased;
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;
    public event EventHandler<OperationEventArgs>? Operation;

    public QueueUri Uri { get; }
    public bool IsStream => false;

    public async ValueTask<bool> IsEmptyAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[is-empty/cancelled]", true));
            return true;
        }

        Operation?.Invoke(this, new("[is-empty/starting]"));

        if (_disposed)
        {
            return true;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            return AccessQueue(() =>
            {
                var result = GetChannel().Model.BasicGet(Uri.QueueName, false);

                var empty = result == null;

                if (result != null)
                {
                    GetChannel().Model.BasicReject(result.DeliveryTag, true);
                }

                Operation?.Invoke(this, new("[is-empty]", empty));

                return empty;
            });
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task EnqueueAsync(TransportMessage transportMessage, Stream stream)
    {
        Guard.AgainstNull(transportMessage);
        Guard.AgainstNull(stream);

        if (_disposed)
        {
            throw new RabbitMQQueueException(string.Format(Resources.QueueDisposed, Uri));
        }

        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[enqueue/cancelled]"));
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
                    data = new(segment.Array, segment.Offset, length);
                }
                else
                {
                    data = stream.ToBytesAsync().GetAwaiter().GetResult();
                }

                model.BasicPublish(string.Empty, Uri.QueueName, false, properties, data);
            });

            MessageEnqueued?.Invoke(this, new(transportMessage, stream));
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[enqueue/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<ReceivedMessage?> GetMessageAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[get-message/cancelled]"));
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
                    MessageReceived?.Invoke(this, new(receivedMessage));
                }

                return receivedMessage;
            }));
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[get-message/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }

        return null;
    }

    public async Task AcknowledgeAsync(object acknowledgementToken)
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[acknowledge/cancelled]"));
            return;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            AccessQueue(() => GetChannel().Acknowledge((DeliveredMessage)acknowledgementToken));

            MessageAcknowledged?.Invoke(this, new(acknowledgementToken));
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[acknowledge/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ReleaseAsync(object acknowledgementToken)
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[release/cancelled]"));
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

            MessageReleased?.Invoke(this, new(acknowledgementToken));
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[release/cancelled]"));
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

    private T? AccessQueue<T>(Func<T> action, int retry = 0)
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
        if (_connection != null && _channel is { Model.IsOpen: true })
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

        _channel = new(model, Uri, _rabbitMQOptions);

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
        Operation?.Invoke(this, new("[queue-declare/starting]"));

        model.QueueDeclare(Uri.QueueName, _rabbitMQOptions.Durable, false, false, _arguments);

        try
        {
            model.QueueDeclarePassive(Uri.QueueName);

            Operation?.Invoke(this, new("[queue-declare/]"));
        }
        catch
        {
            Operation?.Invoke(this, new("[queue-declare/failed]"));
        }
    }
}

internal class DeliveredMessage
{
    public IBasicProperties BasicProperties { get; set; } = null!;
    public byte[] Data { get; set; } = null!;

    public int DataLength { get; set; }

    public ulong DeliveryTag { get; set; }
}