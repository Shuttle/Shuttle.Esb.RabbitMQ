using System;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQBuilder
    {
        private RabbitMQOptions _rabbitMQOptions = new RabbitMQOptions(); 
        
        public RabbitMQOptions Options
        {
            get => _rabbitMQOptions;
            set => _rabbitMQOptions = value ?? throw new ArgumentNullException(nameof(value));
        }

        public RabbitMQBuilder(IServiceCollection services)
        {
            Guard.AgainstNull(services, nameof(services));

            Services = services;
        }

        public IServiceCollection Services { get; }
    }
}