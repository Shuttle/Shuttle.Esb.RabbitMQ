using System;
using RabbitMQ.Client;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ;

public class ConfigureEventArgs
{
    private ConnectionFactory _connectionFactory;

    public ConfigureEventArgs(ConnectionFactory connectionFactory)
    {
        _connectionFactory = Guard.AgainstNull(connectionFactory);
    }

    public ConnectionFactory ConnectionFactory
    {
        get => _connectionFactory;
        set => _connectionFactory = Guard.AgainstNull(value);
    }
}