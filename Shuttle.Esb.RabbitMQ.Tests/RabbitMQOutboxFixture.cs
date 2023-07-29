using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQOutboxFixture : OutboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_be_able_to_use_an_outbox(bool isTransactionalEndpoint)
        {
            await TestOutboxSending(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", 3, isTransactionalEndpoint);
        }
    }
}