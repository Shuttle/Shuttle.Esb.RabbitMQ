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
        private readonly object _lock = new object();
        private readonly int _millisecondsTimeout;

        private readonly BlockingCollection<DeliveredMessage> _queue =
            new BlockingCollection<DeliveredMessage>(new ConcurrentQueue<DeliveredMessage>());

        private readonly QueueUri _uri;
        private volatile bool _consumerAdded;
        private bool _disposed;

        public Channel(IModel model, QueueUri uri, RabbitMQOptions rabbitMOptions)
        {
            Guard.AgainstNull(model, nameof(model));
            Guard.AgainstNull(rabbitMOptions, nameof(rabbitMOptions));

            Model = model;
            _uri = uri;

            _millisecondsTimeout = (int)rabbitMOptions.QueueTimeout.TotalMilliseconds;
        }

        private bool IsOpen => Model?.IsOpen == true;

        public IModel Model { get; }

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

        void IBasicConsumer.HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey,
            IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            if (_disposed)
            {
                return;
            }

            lock (_lock)
            {
                // body should be copied, since it will be accessed later from another thread
                var data = ArrayPool<byte>.Shared.Rent(body.Length);
                body.CopyTo(data);

                _queue.Add(new DeliveredMessage
                {
                    Data = data,
                    DataLength = body.Length,
                    BasicProperties = properties,
                    DeliveryTag = deliveryTag
                });
            }
        }

        void IBasicConsumer.HandleModelShutdown(object model, ShutdownEventArgs reason)
        {
            _consumerAdded = false;
        }

        // not used
        public event EventHandler<ConsumerEventArgs> ConsumerCancelled = delegate
        {
        };

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

        public DeliveredMessage Next()
        {
            EnsureConsumer();

            try
            {
                if (_consumerAdded && !Model.IsClosed &&
                    _queue.TryTake(out var deliveredMessage, _millisecondsTimeout))
                {
                    if (deliveredMessage == null)
                    {
                        throw new ConnectionException(
                            string.Format(Resources.SubscriptionNextConnectionException, _uri));
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
            Model.BasicConsume(_uri.QueueName, false, this);
        }

        public void Acknowledge(DeliveredMessage deliveredMessage)
        {
            EnsureConsumer();

            if (IsOpen)
            {
                Model.BasicAck(deliveredMessage.DeliveryTag, false);
                ArrayPool<byte>.Shared.Return(deliveredMessage.Data);
            }
        }
    }
}