using System;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQDeferredMessageFixture : DeferredFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public async Task Should_be_able_to_perform_full_processing(bool isTransactionalEndpoint)
        {
            await TestDeferredProcessing(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", isTransactionalEndpoint);
        }
    }
}