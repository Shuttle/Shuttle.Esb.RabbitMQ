using NUnit.Framework;
using Shuttle.ESB.Tests;

namespace Shuttle.ESB.RabbitMQ.Tests
{
	public class RabbitMQDeferredMessageTest : DeferredFixture
	{
		[Test]
		[TestCase(false)]
		[TestCase(true)]
		public void Should_be_able_to_perform_full_processing(bool isTransactionalEndpoint)
		{
			TestDeferredProcessing("rabbitmq://shuttle:shuttle!@localhost/{0}", isTransactionalEndpoint);
		}
	}
}