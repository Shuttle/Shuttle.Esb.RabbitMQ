using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
	[TestFixture]
	public class RabbitMQQueueTest : BasicQueueFixture
	{
		[Test]
		public void Should_be_able_to_perform_simple_enqueue_and_get_message()
		{
			TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetComponentContainer(), "rabbitmq://shuttle:shuttle!@localhost/{0}");
			TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetComponentContainer(), "rabbitmq://shuttle:shuttle!@localhost/{0}-transient?durable=false");
		}

		[Test]
		public void Should_be_able_to_release_a_message()
		{
			TestReleaseMessage(RabbitMQFixture.GetComponentContainer(), "rabbitmq://shuttle:shuttle!@localhost/{0}");
		}

		[Test]
		public void Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
		{
			TestUnacknowledgedMessage(RabbitMQFixture.GetComponentContainer(), "rabbitmq://shuttle:shuttle!@localhost/{0}");
		}
	}
}