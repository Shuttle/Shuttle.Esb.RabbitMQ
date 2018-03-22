using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQDistributorFixture : DistributorFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_distribute_messages(bool isTransactionalEndpoint)
        {
            TestDistributor(RabbitMQFixture.GetComponentContainer(), 
                RabbitMQFixture.GetComponentContainer(), @"rabbitmq://shuttle:shuttle!@localhost/{0}", isTransactionalEndpoint);
        }
    }
}