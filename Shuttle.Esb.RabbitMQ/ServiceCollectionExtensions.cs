using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRabbitMQ(this IServiceCollection services,
            Action<RabbitMQBuilder> builder = null)
        {
            Guard.AgainstNull(services, nameof(services));

            var rabbitMQBuilder = new RabbitMQBuilder(services);

            builder?.Invoke(rabbitMQBuilder);

            services.TryAddSingleton<IQueueFactory, RabbitMQQueueFactory>();

            services.AddOptions<RabbitMQOptions>().Configure(options =>
            {
                options.ConnectionCloseTimeout = rabbitMQBuilder.Options.ConnectionCloseTimeout;
                options.DefaultPrefetchCount = rabbitMQBuilder.Options.DefaultPrefetchCount;
                options.LocalQueueTimeout = rabbitMQBuilder.Options.LocalQueueTimeout;
                options.OperationRetryCount = rabbitMQBuilder.Options.OperationRetryCount;
                options.RemoteQueueTimeout = rabbitMQBuilder.Options.RemoteQueueTimeout;
                options.RequestedHeartbeat = rabbitMQBuilder.Options.RequestedHeartbeat;
                options.UseBackgroundThreadsForIO = rabbitMQBuilder.Options.UseBackgroundThreadsForIO;
            });

            return services;
        }
    }
}