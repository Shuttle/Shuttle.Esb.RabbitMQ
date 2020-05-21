using System;
using System.Collections.Concurrent;
using System.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    internal sealed class Channel : IDisposable
    {
        private readonly string _queueName;
        private volatile EventingBasicConsumer _consumer;
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

        public IModel Model { get; private set; }

        private bool IsOpen => Model?.IsOpen == true;

        public DeliveredMessage Next()
        {
            EnsureConsumer();

            try
            {
                if (_consumer != null && !Model.IsClosed && _queue.TryTake(out var deliveredMessage, _millisecondsTimeout))
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
            if (_consumer != null || !IsOpen)
            {
                return;
            }
            
            _consumer = new EventingBasicConsumer(Model);

            _consumer.Received += (sender, args) =>
            {
                lock (_lock)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    // body should be copied, since it will be accessed later from another thread
                    _queue.Add(new DeliveredMessage
                    {
                        Data = args.Body.ToArray(),
                        BasicProperties = args.BasicProperties,
                        DeliveryTag = args.DeliveryTag,
                    });
                }
            };

            Model.BasicConsume(_queueName, false, _consumer);

            _consumer.ConsumerCancelled += (sender, args) => _consumer = null;
        }

        public void Acknowledge(DeliveredMessage deliveredMessage)
        {
            if (deliveredMessage == null)
            {
                return;
            }

            EnsureConsumer();

            if (IsOpen)
            {
                Model.BasicAck(deliveredMessage.DeliveryTag, false);
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
    }
}