﻿using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQInboxFixture : InboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public void Should_be_able_handle_errors(bool isTransactionalEndpoint)
        {
            TestInboxError(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}", isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public void Should_be_able_to_process_messages_concurrently(int msToComplete, bool isTransactionalEndpoint)
        {
            TestInboxConcurrency(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(150, true)]
        [TestCase(150, false)]
        public void Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            TestInboxThroughput(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}", 1000, count, isTransactionalEndpoint);
        }

        [Test]
        public void Should_be_able_to_handle_a_deferred_message()
        {
            TestInboxDeferred(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}");
        }

        [Test]
        public void Should_be_able_to_expire_a_message()
        {
            TestInboxExpiry(RabbitMQFixture.GetServiceCollection(), "rabbitmq://shuttle:shuttle!@localhost/{0}");
        }
    }
}