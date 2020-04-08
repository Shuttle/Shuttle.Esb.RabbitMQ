using System;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQConfiguration : IRabbitMQConfiguration
    {
        public RabbitMQConfiguration()
        {
            RequestedHeartbeat = TimeSpan.FromSeconds(30);
            LocalQueueTimeoutMilliseconds = 250;
            RemoteQueueTimeoutMilliseconds = 1000;
            ConnectionCloseTimeout = TimeSpan.FromSeconds(1);
            OperationRetryCount = 3;
            DefaultPrefetchCount = 25;
            UseBackgroundThreadsForIO = true;
        }

        public TimeSpan RequestedHeartbeat { get; set; }
        public int LocalQueueTimeoutMilliseconds { get; set; }
        public int RemoteQueueTimeoutMilliseconds { get; set; }
        public TimeSpan ConnectionCloseTimeout { get; set; }
        public int OperationRetryCount { get; set; }
        public ushort DefaultPrefetchCount { get; set; }
        public bool UseBackgroundThreadsForIO { get; set; }
    }
}