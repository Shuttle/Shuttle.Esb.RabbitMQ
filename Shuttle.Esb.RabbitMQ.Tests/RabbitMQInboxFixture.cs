using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQInboxFixture : InboxFixture
    {
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public async Task Should_be_able_handle_errors(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            await TestInboxError(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public async Task Should_be_able_to_process_messages_concurrently(int msToComplete, bool isTransactionalEndpoint)
        {
            await TestInboxConcurrency(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(150, true)]
        [TestCase(150, false)]
        public async Task Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            await TestInboxThroughput(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", 1000, count, 1, isTransactionalEndpoint);
        }

        [Test]
        public async Task Should_be_able_to_handle_a_deferred_message()
        {
            await TestInboxDeferred(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}", TimeSpan.FromMilliseconds(500));
        }

        [Test]
        public async Task Should_be_able_to_expire_a_message()
        {
            await TestInboxExpiry(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }
    }
}