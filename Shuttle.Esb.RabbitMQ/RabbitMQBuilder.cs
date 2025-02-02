using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ;

public class RabbitMQBuilder
{
    internal readonly Dictionary<string, RabbitMQOptions> RabbitMQOptions = new();

    public RabbitMQBuilder(IServiceCollection services)
    {
        Services = Guard.AgainstNull(services);
    }

    public IServiceCollection Services { get; }

    public RabbitMQBuilder AddOptions(string name, RabbitMQOptions amazonSqsOptions)
    {
        Guard.AgainstNullOrEmptyString(name);
        Guard.AgainstNull(amazonSqsOptions);

        RabbitMQOptions.Remove(name);

        RabbitMQOptions.Add(name, amazonSqsOptions);

        return this;
    }
}