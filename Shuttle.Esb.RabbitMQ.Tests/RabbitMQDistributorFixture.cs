using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQDistributorFixture : DistributorFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public async Task Should_be_able_to_distribute_messages(bool isTransactionalEndpoint)
        {
            await TestDistributor(RabbitMQFixture.GetServiceCollection(), 
                RabbitMQFixture.GetServiceCollection(), @"rabbitmq://local/{0}", isTransactionalEndpoint);
        }
    }
}