using System;

namespace Shuttle.Esb.RabbitMQ
{
	internal class ConnectionException : Exception
	{
		public ConnectionException(string message) : base(message)
		{
		}
	}
}