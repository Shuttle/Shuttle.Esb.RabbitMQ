using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQQueueFactory : IQueueFactory
    {
        private readonly IOptionsMonitor<RabbitMQOptions> _rabbitMQOptions;

        public RabbitMQQueueFactory(IOptionsMonitor<RabbitMQOptions> rabbitMQOptions)
        {
            Guard.AgainstNull(rabbitMQOptions, nameof(rabbitMQOptions));

            _rabbitMQOptions = rabbitMQOptions;
        }

        public string Scheme => "rabbitmq";

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            var queueUri = new QueueUri(uri).SchemeInvariant(Scheme);
            var rabbitMQOptions = _rabbitMQOptions.Get(queueUri.ConfigurationName);

            if (rabbitMQOptions == null)
            {
                throw new InvalidOperationException(string.Format(Esb.Resources.QueueConfigurationNameException, queueUri.ConfigurationName));
            }

            return new RabbitMQQueue(queueUri, rabbitMQOptions);
        }
    }
}