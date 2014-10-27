using NUnit.Framework;
using Shuttle.ESB.Tests;

namespace Shuttle.ESB.RabbitMQ.Tests
{
	public class RabbitMQOutboxTest : OutboxFixture
	{
		[TestCase(true)]
		[TestCase(false)]
		public void Should_be_able_handle_errors(bool isTransactionalEndpoint)
		{
			TestOutboxSending("rabbitmq://shuttle:shuttle!@localhost/{0}", isTransactionalEndpoint);
		}
	}
}