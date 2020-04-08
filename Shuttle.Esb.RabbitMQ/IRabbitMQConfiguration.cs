using System;

namespace Shuttle.Esb.RabbitMQ
{
    public interface IRabbitMQConfiguration
    {
        TimeSpan RequestedHeartbeat { get; set; }
        int LocalQueueTimeoutMilliseconds { get; set; }
        int RemoteQueueTimeoutMilliseconds { get; set; }
        TimeSpan ConnectionCloseTimeout { get; set; }
        int OperationRetryCount { get; set; }
        ushort DefaultPrefetchCount { get; set; }
        bool UseBackgroundThreadsForIO { get; set; }
    }
}