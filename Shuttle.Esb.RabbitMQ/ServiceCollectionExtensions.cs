using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMQ(this IServiceCollection services, Action<RabbitMQBuilder>? builder = null)
    {
        Guard.AgainstNull(services);

        var rabbitMQBuilder = new RabbitMQBuilder(services);

        builder?.Invoke(rabbitMQBuilder);

        services.AddSingleton<IValidateOptions<RabbitMQOptions>, RabbitMQOptionsValidator>();

        foreach (var pair in rabbitMQBuilder.RabbitMQOptions)
        {
            services.AddOptions<RabbitMQOptions>(pair.Key).Configure(options =>
            {
                options.ConnectionCloseTimeout = pair.Value.ConnectionCloseTimeout;
                options.QueueTimeout = pair.Value.QueueTimeout;
                options.OperationRetryCount = pair.Value.OperationRetryCount;
                options.RequestedHeartbeat = pair.Value.RequestedHeartbeat;
                options.Priority = pair.Value.Priority;
                options.Host = pair.Value.Host;
                options.VirtualHost = pair.Value.VirtualHost;
                options.Port = pair.Value.Port;
                options.Username = pair.Value.Username;
                options.Password = pair.Value.Password;
                options.Persistent = pair.Value.Persistent;
                options.PrefetchCount = pair.Value.PrefetchCount;
                options.Durable = pair.Value.Durable;

                if (options.PrefetchCount < 0)
                {
                    options.PrefetchCount = 0;
                }

                options.Configure += (sender, args) =>
                {
                    pair.Value.OnConfigureConsumer(sender, args);
                };
            });
        }

        services.TryAddSingleton<IQueueFactory, RabbitMQQueueFactory>();

        return services;
    }
}