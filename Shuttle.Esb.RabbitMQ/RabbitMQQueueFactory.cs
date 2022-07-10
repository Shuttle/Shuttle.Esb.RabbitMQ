using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQQueueFactory : IQueueFactory
    {
        private readonly RabbitMQOptions _rabbitMQOptions;

        public RabbitMQQueueFactory(IOptions<RabbitMQOptions> rabbitMQOptions)
        {
            Guard.AgainstNull(rabbitMQOptions, nameof(rabbitMQOptions));
            Guard.AgainstNull(rabbitMQOptions.Value, nameof(rabbitMQOptions.Value));

            _rabbitMQOptions = rabbitMQOptions.Value;
        }

        public string Scheme => RabbitMQUriParser.Scheme;

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new RabbitMQQueue(uri, _rabbitMQOptions);
        }
    }
}