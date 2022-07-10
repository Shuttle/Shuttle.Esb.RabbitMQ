using System;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQBuilder
    {
        private RabbitMQOptions _serviceBusOptions = new RabbitMQOptions(); 
        
        public RabbitMQOptions Options
        {
            get => _serviceBusOptions;
            set => _serviceBusOptions = value ?? throw new ArgumentNullException(nameof(value));
        }

        public RabbitMQBuilder(IServiceCollection services)
        {
            Guard.AgainstNull(services, nameof(services));

            Services = services;
        }

        public IServiceCollection Services { get; }
    }
}