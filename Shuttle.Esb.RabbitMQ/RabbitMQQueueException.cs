using System;

namespace Shuttle.Esb.RabbitMQ
{
	public class RabbitMQQueueException : Exception
	{
		public RabbitMQQueueException(string message) : base(message)
		{
		}
	}
}