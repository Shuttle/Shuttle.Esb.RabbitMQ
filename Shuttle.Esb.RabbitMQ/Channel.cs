using System;
using System.Runtime.InteropServices;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    internal sealed class Channel : IDisposable
    {
        private readonly string _queue;
        private Subscription _subscription;
        private readonly int _millisecondsTimeout;

        public Channel(IModel model, RabbitMQUriParser parser, IRabbitMQConfiguration configuration)
        {
            Guard.AgainstNull(model, nameof(model));
            Guard.AgainstNull(parser, nameof(parser));
            Guard.AgainstNull(configuration, nameof(configuration));

            Model = model;

            _queue = parser.Queue;

            _millisecondsTimeout = parser.Local
                ? configuration.LocalQueueTimeoutMilliseconds
                : configuration.RemoteQueueTimeoutMilliseconds;
        }

        public IModel Model { get; }

        public BasicDeliverEventArgs Next()
        {
            var next = GetSubscription().Next(_millisecondsTimeout, out var basicDeliverEventArgs);

            if (next && basicDeliverEventArgs == null)
            {
                throw new ConnectionException(string.Format(Resources.SubscriptionNextConnectionException,
                    _subscription.QueueName));
            }

            return (next)
                ? basicDeliverEventArgs
                : null;
        }

        private Subscription GetSubscription()
        {
            return _subscription ?? (_subscription = new Subscription(Model, _queue, false));
        }

        public void Acknowledge(BasicDeliverEventArgs basicDeliverEventArgs)
        {
            GetSubscription().Ack(basicDeliverEventArgs);
        }

        public void Dispose()
        {
            try
            {
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