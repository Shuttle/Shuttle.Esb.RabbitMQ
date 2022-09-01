using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQDeferredMessageFixture : DeferredFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_perform_full_processing(bool isTransactionalEndpoint)
        {
            TestDeferredProcessing(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", isTransactionalEndpoint);
        }
    }
}