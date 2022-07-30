using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQBuilder
    {
        internal readonly Dictionary<string, RabbitMQOptions> RabbitMQOptions = new Dictionary<string, RabbitMQOptions>();

        public RabbitMQBuilder(IServiceCollection services)
        {
            Guard.AgainstNull(services, nameof(services));

            Services = services;
        }

        public IServiceCollection Services { get; }

        public RabbitMQBuilder AddOptions(string name, RabbitMQOptions amazonSqsOptions)
        {
            Guard.AgainstNullOrEmptyString(name, nameof(name));
            Guard.AgainstNull(amazonSqsOptions, nameof(amazonSqsOptions));

            RabbitMQOptions.Remove(name);

            RabbitMQOptions.Add(name, amazonSqsOptions);

            return this;
        }
    }
}