using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Pipelines;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public static class RabbitMQFixture
    {
        public static IServiceCollection GetServiceCollection()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

            services.AddRabbitMQ(builder =>
            {
                builder.AddOptions("local", new RabbitMQOptions
                {
                    Host = "127.0.0.1",
                    Username = "shuttle",
                    Password = "shuttle!",
                    PrefetchCount = 15,
                    QueueTimeout = TimeSpan.FromMilliseconds(25),
                    ConnectionCloseTimeout = TimeSpan.FromMilliseconds(25)
                });
            });

            return services;
        }
    }
}