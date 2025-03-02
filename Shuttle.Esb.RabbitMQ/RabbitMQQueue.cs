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
    private readonly Dictionary<string, object?> _arguments = new();
    private readonly CancellationToken _cancellationToken;

    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private readonly int _operationRetryCount;
    private readonly RabbitMQOptions _rabbitMQOptions;

    private RawChannel? _channel;
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
            await AccessQueueAsync(async () => await QueueDeclareAsync((await GetRawChannelAsync()).Channel));
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

            CloseConnectionAsync().GetAwaiter().GetResult();
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
            await AccessQueueAsync(async () =>
            {
                await (await GetRawChannelAsync()).Channel.QueueDeleteAsync(Uri.QueueName, cancellationToken: _cancellationToken);
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
            await AccessQueueAsync(async () =>
            {
                await (await GetRawChannelAsync()).Channel.QueuePurgeAsync(Uri.QueueName, _cancellationToken);
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
            return await AccessQueueAsync(async () =>
            {
                var result = await (await GetRawChannelAsync()).Channel.BasicGetAsync(Uri.QueueName, false, _cancellationToken);

                var empty = result == null;

                if (result != null)
                {
                    await (await GetRawChannelAsync()).Channel.BasicRejectAsync(result.DeliveryTag, true, _cancellationToken);
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
            await AccessQueueAsync(async () =>
            {
                var channel = (await GetRawChannelAsync()).Channel;

                var properties = new BasicProperties
                {
                    Persistent = _rabbitMQOptions.Persistent,
                    CorrelationId = transportMessage.MessageId.ToString()
                };

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

                await channel.BasicPublishAsync(string.Empty, Uri.QueueName, false, properties, data, _cancellationToken);
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
            return await AccessQueueAsync(async () =>
            {
                var result = await (await GetRawChannelAsync()).NextAsync();

                var receivedMessage = result == null
                    ? null
                    : new ReceivedMessage(new MemoryStream(result.Data, 0, result.DataLength, false, true), result);

                if (receivedMessage != null)
                {
                    MessageReceived?.Invoke(this, new(receivedMessage));
                }

                return receivedMessage;
            });
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
            await AccessQueueAsync(async () => await (await GetRawChannelAsync()).AcknowledgeAsync((DeliveredMessage)acknowledgementToken));

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
            await AccessQueueAsync(async () =>
            {
                var token = (DeliveredMessage)acknowledgementToken;

                var rawChannel = await GetRawChannelAsync();

                await rawChannel.Channel.BasicPublishAsync(string.Empty, Uri.QueueName, false, token.BasicProperties, token.Data.AsMemory(0, token.DataLength), _cancellationToken).ConfigureAwait(false);
                await rawChannel.AcknowledgeAsync(token);
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

    private async Task AccessQueueAsync(Func<Task> action, int retry = 0)
    {
        if (_disposed || _cancellationToken.IsCancellationRequested)
        {
            return;
        }

        try
        {
            await action.Invoke();
        }
        catch (ConnectionException)
        {
            if (retry == _operationRetryCount)
            {
                throw;
            }

            await CloseConnectionAsync();

            await AccessQueueAsync(action, retry + 1);
        }
    }

    private async Task<T?> AccessQueueAsync<T>(Func<Task<T>> action, int retry = 0)
    {
        if (_disposed)
        {
            return default;
        }

        try
        {
            return await action.Invoke();
        }
        catch (ConnectionException)
        {
            if (retry == 3)
            {
                throw;
            }

            await CloseConnectionAsync();

            return await AccessQueueAsync(action, retry + 1);
        }
    }

    private async Task CloseConnectionAsync()
    {
        if (_connection == null)
        {
            return;
        }

        if (_connection.IsOpen)
        {
            await _connection.CloseAsync(_rabbitMQOptions.ConnectionCloseTimeout);
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

    private async Task<RawChannel> GetRawChannelAsync()
    {
        if (_connection != null && _channel is { Channel.IsOpen: true })
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
                _connection = await GetConnectionAsync();
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

        var channel = await _connection.CreateChannelAsync(cancellationToken: _cancellationToken);

        await channel.BasicQosAsync(0, _rabbitMQOptions.PrefetchCount, false, _cancellationToken);

        await QueueDeclareAsync(channel);

        _channel = new(channel, Uri, _rabbitMQOptions);

        return _channel;
    }

    private async Task<IConnection> GetConnectionAsync()
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

            await CloseConnectionAsync();
        }

        return await _factory.CreateConnectionAsync(Uri.QueueName, _cancellationToken);
    }

    private async Task QueueDeclareAsync(IChannel channel)
    {
        Operation?.Invoke(this, new("[queue-declare/starting]"));

        await channel.QueueDeclareAsync(Uri.QueueName, _rabbitMQOptions.Durable, false, false, _arguments, cancellationToken: _cancellationToken);

        try
        {
            await channel.QueueDeclarePassiveAsync(Uri.QueueName, _cancellationToken);

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
    public BasicProperties BasicProperties { get; set; } = null!;
    public byte[] Data { get; set; } = null!;

    public int DataLength { get; set; }

    public ulong DeliveryTag { get; set; }
}