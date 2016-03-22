namespace Shuttle.Esb.RabbitMQ
{
	public class RabbitMQConfiguration : IRabbitMQConfiguration
	{
		public RabbitMQConfiguration()
		{
			RequestedHeartbeat = 30;
			LocalQueueTimeoutMilliseconds = 250;
			RemoteQueueTimeoutMilliseconds = 1000;
			ConnectionCloseTimeoutMilliseconds = 1000;
			OperationRetryCount = 3;
			DefaultPrefetchCount = 25;
		}

		public ushort RequestedHeartbeat { get; set; }
		public int LocalQueueTimeoutMilliseconds { get; set; }
		public int RemoteQueueTimeoutMilliseconds { get; set; }
		public int ConnectionCloseTimeoutMilliseconds { get; set; }
		public int OperationRetryCount { get; set; }
		public ushort DefaultPrefetchCount { get; set; }
	}
}