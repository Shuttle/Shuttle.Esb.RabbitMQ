using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ;

public class RabbitMQOptions
{
    public const string SectionName = "Shuttle:RabbitMQ";
    public TimeSpan ConnectionCloseTimeout { get; set; } = TimeSpan.FromSeconds(1);
    public bool Durable { get; set; } = true;
    public string Host { get; set; } = string.Empty;
    public int OperationRetryCount { get; set; } = 3;
    public string Password { get; set; } = string.Empty;
    public bool Persistent { get; set; } = true;
    public int Port { get; set; } = -1;
    public ushort PrefetchCount { get; set; } = 25;
    public int Priority { get; set; }
    public TimeSpan QueueTimeout { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);
    public string Username { get; set; } = string.Empty;
    public string VirtualHost { get; set; } = "/";

    public event EventHandler<ConfigureEventArgs>? Configure;

    public void OnConfigureConsumer(object? sender, ConfigureEventArgs args)
    {
        Configure?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }
}