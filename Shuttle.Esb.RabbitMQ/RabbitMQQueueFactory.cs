using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.RabbitMQ;

public class RabbitMQQueueFactory : IQueueFactory
{
    private readonly ICancellationTokenSource _cancellationTokenSource;
    private readonly IOptionsMonitor<RabbitMQOptions> _rabbitMQOptions;

    public RabbitMQQueueFactory(IOptionsMonitor<RabbitMQOptions> rabbitMQOptions, ICancellationTokenSource cancellationTokenSource)
    {
        _rabbitMQOptions = Guard.AgainstNull(rabbitMQOptions);
        _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource);
    }

    public string Scheme => "rabbitmq";

    public IQueue Create(Uri uri)
    {
        var queueUri = new QueueUri(Guard.AgainstNull(uri)).SchemeInvariant(Scheme);
        var rabbitMQOptions = _rabbitMQOptions.Get(queueUri.ConfigurationName);

        if (rabbitMQOptions == null)
        {
            throw new InvalidOperationException(string.Format(Esb.Resources.QueueConfigurationNameException, queueUri.ConfigurationName));
        }

        return new RabbitMQQueue(queueUri, rabbitMQOptions, _cancellationTokenSource.Get().Token);
    }
}