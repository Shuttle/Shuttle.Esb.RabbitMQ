using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    internal sealed class Channel : IBasicConsumer, IDisposable
    {
        private readonly string _queueName;
        private volatile bool _consumerAdded;
        private readonly BlockingCollection<DeliveredMessage> _queue =
            new BlockingCollection<DeliveredMessage>(new ConcurrentQueue<DeliveredMessage>());
        private readonly int _millisecondsTimeout;
        private readonly object _lock = new object();
        private bool _disposed;

        public Channel(IModel model, RabbitMQUriParser parser, IRabbitMQConfiguration configuration)
        {
            Guard.AgainstNull(model, nameof(model));
            Guard.AgainstNull(parser, nameof(parser));
            Guard.AgainstNull(configuration, nameof(configuration));

            Model = model;
            
            _queueName = parser.Queue;

            _millisecondsTimeout = parser.Local
                ? configuration.LocalQueueTimeoutMilliseconds
                : configuration.RemoteQueueTimeoutMilliseconds;
        }

        public IModel Model { get; }

        private bool IsOpen => Model?.IsOpen == true;

        public DeliveredMessage Next()
        {
            EnsureConsumer();

            try
            {
                if (_consumerAdded && !Model.IsClosed && _queue.TryTake(out var deliveredMessage, _millisecondsTimeout))
                {
                    if (deliveredMessage == null)
                    {
                        throw new ConnectionException(string.Format(Resources.SubscriptionNextConnectionException, _queueName));
                    }

                    return deliveredMessage;
                }
            }
            catch (EndOfStreamException)
            {
            }

            return null;
        }

        private void EnsureConsumer()
        {
            if (_consumerAdded || !IsOpen)
            {
                return;
            }

            _consumerAdded = true;
            Model.BasicConsume(_queueName, false, this);
        }

        public void Acknowledge(DeliveredMessage deliveredMessage)
        {
            EnsureConsumer();

            if (IsOpen)
            {
                Model.BasicAck(deliveredMessage.DeliveryTag, false);
#if NETSTANDARD2_1
                deliveredMessage.Stream.Dispose(); // return stream to cache
#else
                ArrayPool<byte>.Shared.Return(deliveredMessage.Data);
#endif
            }
        }

        public void Dispose()
        {
            try
            {
                lock (_lock)
                {
                    _queue.Dispose();

                    if (Model.IsOpen)
                    {
                        Model.Close();
                    }

                    Model.Dispose();

                    _disposed = true;
                }
            }
            catch
            {
                // ignored
            }
        }

        void IBasicConsumer.HandleBasicCancel(string consumerTag)
        {
            _consumerAdded = false;
        }

        void IBasicConsumer.HandleBasicCancelOk(string consumerTag)
        {
            _consumerAdded = false;
        }

        void IBasicConsumer.HandleBasicConsumeOk(string consumerTag)
        {
        }

        void IBasicConsumer.HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey,
            IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            if (_disposed)
            {
                return;
            }

            lock (_lock)
            {
                // body should be copied, since it will be accessed later from another thread
                // in netstandard 2.0 copy to a rented array, in netstandard 2.1+ copy to a cached memory stream
#if NETSTANDARD2_1
                var stream = MemoryStreamCache.Manager.GetStream();
                stream.Write(body.Span);
#else
                var data = ArrayPool<byte>.Shared.Rent(body.Length);
                body.CopyTo(data);
#endif

                _queue.Add(new DeliveredMessage
                {
#if NETSTANDARD2_1
                    Stream = stream,
#else
                    Data = data,
                    DataLength = body.Length,
#endif
                    BasicProperties = properties,
                    DeliveryTag = deliveryTag,
                });
            }
        }

        void IBasicConsumer.HandleModelShutdown(object model, ShutdownEventArgs reason)
        {
            _consumerAdded = false;
        }

        // not used
        public event EventHandler<ConsumerEventArgs> ConsumerCancelled = delegate { };
    }
}