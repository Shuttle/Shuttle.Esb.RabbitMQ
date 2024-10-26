using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests;

[TestFixture]
public class RabbitMQQueueFixture : BasicQueueFixture
{
    [Test]
    public async Task Should_be_able_to_perform_simple_enqueue_and_get_message_async()
    {
        await TestSimpleEnqueueAndGetMessageAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        await TestSimpleEnqueueAndGetMessageAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}-transient?durable=false");
    }

    [Test]
    public async Task Should_be_able_to_release_a_message_async()
    {
        await TestReleaseMessageAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
    }

    [Test]
    public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
    {
        await TestUnacknowledgedMessageAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
    }
}