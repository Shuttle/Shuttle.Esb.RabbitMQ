using System;
using System.Configuration;
using Shuttle.Core.Configuration;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQSection : ConfigurationSection
    {
        [ConfigurationProperty("requestedHeartbeat", IsRequired = false, DefaultValue = (ushort) 30)]
        public ushort RequestedHeartbeat => (ushort) this["requestedHeartbeat"];

        [ConfigurationProperty("localQueueTimeoutMilliseconds", IsRequired = false, DefaultValue = 250)]
        public int LocalQueueTimeoutMilliseconds => (int) this["localQueueTimeoutMilliseconds"];

        [ConfigurationProperty("remoteQueueTimeoutMilliseconds", IsRequired = false, DefaultValue = 1000)]
        public int RemoteQueueTimeoutMilliseconds => (int) this["remoteQueueTimeoutMilliseconds"];

        [ConfigurationProperty("connectionCloseTimeoutMilliseconds", IsRequired = false, DefaultValue = 1000)]
        public int ConnectionCloseTimeoutMilliseconds => (int) this["connectionCloseTimeoutMilliseconds"];

        [ConfigurationProperty("operationRetryCount", IsRequired = false, DefaultValue = 3)]
        public int OperationRetryCount => (int) this["operationRetryCount"];

        [ConfigurationProperty("defaultPrefetchCount", IsRequired = false, DefaultValue = (ushort) 25)]
        public ushort DefaultPrefetchCount => (ushort) this["defaultPrefetchCount"];

        [ConfigurationProperty("useBackgroundThreadsForIO", IsRequired = false, DefaultValue = true)]
        public bool UseBackgroundThreadsForIO => (bool) this["useBackgroundThreadsForIO"];

        public static RabbitMQConfiguration Configuration()
        {
            var section = ConfigurationSectionProvider.Open<RabbitMQSection>("shuttle", "rabbitmq");
            var configuration = new RabbitMQConfiguration();

            if (section != null)
            {
                configuration.RequestedHeartbeat = TimeSpan.FromSeconds(section.RequestedHeartbeat);
                configuration.LocalQueueTimeoutMilliseconds = section.LocalQueueTimeoutMilliseconds;
                configuration.RemoteQueueTimeoutMilliseconds = section.RemoteQueueTimeoutMilliseconds;
                configuration.ConnectionCloseTimeout = TimeSpan.FromMilliseconds(section.ConnectionCloseTimeoutMilliseconds);
                configuration.OperationRetryCount = section.OperationRetryCount;
                configuration.DefaultPrefetchCount = section.DefaultPrefetchCount;
                configuration.UseBackgroundThreadsForIO = section.UseBackgroundThreadsForIO;
            }

            return configuration;
        }
    }
}