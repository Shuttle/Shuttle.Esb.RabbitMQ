using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests;

public class RabbitMQOutboxFixture : OutboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_use_an_outbox_async(bool isTransactionalEndpoint)
    {
        await TestOutboxSendingAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", 3, isTransactionalEndpoint);
    }
}