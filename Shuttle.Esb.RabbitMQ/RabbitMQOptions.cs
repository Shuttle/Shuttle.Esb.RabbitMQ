using System;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQOptions
    {
        public const string SectionName = "Shuttle:RabbitMQ";

        public RabbitMQOptions()
        {
            RequestedHeartbeat = TimeSpan.FromSeconds(30);
            LocalQueueTimeout = TimeSpan.FromMilliseconds(250);
            RemoteQueueTimeout = TimeSpan.FromSeconds(1);
            ConnectionCloseTimeout = TimeSpan.FromSeconds(1);
            OperationRetryCount = 3;
            DefaultPrefetchCount = 25;
            UseBackgroundThreadsForIO = true;
        }

        public TimeSpan RequestedHeartbeat { get; set; }
        public TimeSpan LocalQueueTimeout { get; set; }
        public TimeSpan RemoteQueueTimeout { get; set; }
        public TimeSpan ConnectionCloseTimeout { get; set; }
        public int OperationRetryCount { get; set; }
        public ushort DefaultPrefetchCount { get; set; }
        public bool UseBackgroundThreadsForIO { get; set; }
    }
}