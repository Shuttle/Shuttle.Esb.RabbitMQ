using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQQueueFixture : BasicQueueFixture
    {
        [Test]
        public void Should_be_able_to_perform_simple_enqueue_and_get_message()
        {
            TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
            TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}-transient?durable=false");
        }

        [Test]
        public void Should_be_able_to_release_a_message()
        {
            TestReleaseMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }

        [Test]
        public void Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
        {
            TestUnacknowledgedMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }
    }
}