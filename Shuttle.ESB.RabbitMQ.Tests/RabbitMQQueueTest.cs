using NUnit.Framework;
using Shuttle.ESB.Tests;

namespace Shuttle.ESB.RabbitMQ.Tests
{
	[TestFixture]
	public class RabbitMQQueueTest : BasicQueueFixture
	{
		[Test]
		public void Should_be_able_to_perform_simple_enqueue_and_get_message()
		{
			TestSimpleEnqueueAndGetMessage("rabbitmq://shuttle:shuttle!@localhost/{0}");
			TestSimpleEnqueueAndGetMessage("rabbitmq://shuttle:shuttle!@localhost/{0}-transient?durable=false");
		}

		[Test]
		public void Should_be_able_to_release_a_message()
		{
			TestReleaseMessage("rabbitmq://shuttle:shuttle!@localhost/{0}");
		}

		[Test]
		public void Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
		{
			TestUnacknowledgedMessage("rabbitmq://shuttle:shuttle!@localhost/{0}");
		}
	}
}