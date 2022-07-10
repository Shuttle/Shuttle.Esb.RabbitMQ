using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQOutboxFixture : OutboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public void Should_be_able_handle_errors(bool isTransactionalEndpoint)
        {
            TestOutboxSending(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}", isTransactionalEndpoint);
        }
    }
}