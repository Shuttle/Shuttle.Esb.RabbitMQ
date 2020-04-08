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
        private readonly BlockingCollection<AcknowledgementToken> _queue = 
            new BlockingCollection<AcknowledgementToken>(new ConcurrentQueue<AcknowledgementToken>());
        private readonly int _millisecondsTimeout;

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

        public AcknowledgementToken Next()
        {
            EnsureConsumer();

            try
            {
                var consumer = _consumer;
                if (consumer != null && !Model.IsClosed && _queue.TryTake(out var basicDeliverEventArgs, _millisecondsTimeout))
                {
                    if (basicDeliverEventArgs == null)
                    {
                        throw new ConnectionException(string.Format(Resources.SubscriptionNextConnectionException,
                            _queueName));
                    }

                    return basicDeliverEventArgs;
                }
            }
            catch (EndOfStreamException)
            {
            }
        
            return null;
        }

        private void EnsureConsumer()
        {
            if (_consumer != null)
            {
                return;
            }
            
            _consumer = new EventingBasicConsumer(Model);
            _consumer.Received += (sender, args) =>
            {
                // body should be copied, since it will be accessed later from another thread
                var token = new AcknowledgementToken
                {
                    Data = args.Body.ToArray(),
                    BasicProperties = args.BasicProperties,
                    DeliveryTag = args.DeliveryTag,
                };
                _queue.Add(token);
            }; 
            string consumerTag = Model.BasicConsume(_queueName, false, _consumer);
            _consumer.ConsumerCancelled += (sender, args) => _consumer = null;
        }

        public void Acknowledge(AcknowledgementToken acknowledgementToken)
        {
            if (acknowledgementToken == null)
            {
                return;
            }

            EnsureConsumer();
            if (Model.IsOpen)
            {
                Model.BasicAck(acknowledgementToken.DeliveryTag, false);
            }
        }

        public void Dispose()
        {
            try
            {
                _queue.Dispose();

                if (Model.IsOpen)
                {
                    Model.Close();
                }

                Model.Dispose();
            }
            catch
            {
                // ignored
            }
        }
    }
}