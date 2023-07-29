using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQQueueFixture : BasicQueueFixture
    {
        [Test]
        public async Task Should_be_able_to_perform_simple_enqueue_and_get_message()
        {
            await TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
            await TestSimpleEnqueueAndGetMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}-transient?durable=false");
        }

        [Test]
        public async Task Should_be_able_to_release_a_message()
        {
            await TestReleaseMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }

        [Test]
        public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
        {
            await TestUnacknowledgedMessage(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }
    }
}