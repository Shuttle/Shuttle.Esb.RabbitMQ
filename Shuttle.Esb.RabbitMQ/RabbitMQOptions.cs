using System;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQOptions
    {
        public const string SectionName = "Shuttle:RabbitMQ";

        public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan QueueTimeout { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan ConnectionCloseTimeout { get; set; } = TimeSpan.FromSeconds(1);
        public int OperationRetryCount { get; set; } = 3;
        public bool UseBackgroundThreadsForIO { get; set; } = true;
        public int Priority { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Host { get; set; }
        public string VirtualHost { get; set; } = "/";
        public int Port { get; set; } = -1;
        public bool Persistent { get; set; } = true;
        public ushort PrefetchCount { get; set; } = 25;
        public bool Durable { get; set; } = true;
    }
}