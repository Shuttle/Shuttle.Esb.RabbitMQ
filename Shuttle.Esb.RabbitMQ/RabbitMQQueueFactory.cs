using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQQueueFactory : IQueueFactory
    {
        public IRabbitMQConfiguration Configuration { get; }

        public RabbitMQQueueFactory(IRabbitMQConfiguration configuration)
        {
            Configuration = configuration;
        }

        public string Scheme => RabbitMQUriParser.Scheme;

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new RabbitMQQueue(uri, Configuration);
        }

        public bool CanCreate(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return Scheme.Equals(uri.Scheme, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}