using System.Configuration;
using Shuttle.Core.Infrastructure;

namespace Shuttle.ESB.RabbitMQ
{
	public class RabbitMQSection : ConfigurationSection
	{
		[ConfigurationProperty("requestedHeartbeat", IsRequired = false, DefaultValue = (ushort) 30)]
		public ushort RequestedHeartbeat
		{
			get { return (ushort) this["requestedHeartbeat"]; }
		}

		[ConfigurationProperty("localQueueTimeoutMilliseconds", IsRequired = false, DefaultValue = 250)]
		public int LocalQueueTimeoutMilliseconds
		{
			get { return (int) this["localQueueTimeoutMilliseconds"]; }
		}

		[ConfigurationProperty("remoteQueueTimeoutMilliseconds", IsRequired = false, DefaultValue = 1000)]
		public int RemoteQueueTimeoutMilliseconds
		{
			get { return (int) this["remoteQueueTimeoutMilliseconds"]; }
		}

		[ConfigurationProperty("connectionCloseTimeoutMilliseconds", IsRequired = false, DefaultValue = 1000)]
		public int ConnectionCloseTimeoutMilliseconds
		{
			get { return (int) this["connectionCloseTimeoutMilliseconds"]; }
		}

		[ConfigurationProperty("operationRetryCount", IsRequired = false, DefaultValue = 3)]
		public int OperationRetryCount
		{
			get { return (int) this["operationRetryCount"]; }
		}

		[ConfigurationProperty("defaultPrefetchCount", IsRequired = false, DefaultValue = (ushort) 25)]
		public ushort DefaultPrefetchCount
		{
			get { return (ushort) this["defaultPrefetchCount"]; }
		}

		public static RabbitMQConfiguration Configuration()
		{
			var section = ConfigurationSectionProvider.Open<RabbitMQSection>("shuttle", "rabbitmq");
			var configuration = new RabbitMQConfiguration();

			if (section != null)
			{
				configuration.RequestedHeartbeat = section.RequestedHeartbeat;
				configuration.LocalQueueTimeoutMilliseconds = section.LocalQueueTimeoutMilliseconds;
				configuration.RemoteQueueTimeoutMilliseconds = section.RemoteQueueTimeoutMilliseconds;
				configuration.ConnectionCloseTimeoutMilliseconds = section.ConnectionCloseTimeoutMilliseconds;
				configuration.OperationRetryCount = section.OperationRetryCount;
				configuration.DefaultPrefetchCount = section.DefaultPrefetchCount;
			}

			return configuration;
		}
	}
}