using System;
using System.IO;
using NUnit.Framework;
using Shuttle.Core.Configuration;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQSectionFixture
    {
        protected RabbitMQSection GetRabbitMQSection(string file)
        {
            return ConfigurationSectionProvider.OpenFile<RabbitMQSection>("shuttle", "rabbitmq",
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, string.Format(@".\RabbitMQSection\files\{0}", file)));
        }

        [Test]
        [TestCase("RabbitMQ.config")]
        [TestCase("RabbitMQ-Grouped.config")]
        public void Should_be_able_to_load_a_full_configuration(string file)
        {
            var section = GetRabbitMQSection(file);

            Assert.IsNotNull(section);

            Assert.AreEqual(50, section.RequestedHeartbeat);
            Assert.AreEqual(1500, section.LocalQueueTimeoutMilliseconds);
            Assert.AreEqual(3500, section.RemoteQueueTimeoutMilliseconds);
            Assert.AreEqual(1500, section.ConnectionCloseTimeoutMilliseconds);
            Assert.AreEqual(5, section.OperationRetryCount);
            Assert.AreEqual(100, section.DefaultPrefetchCount);
        }
    }
}