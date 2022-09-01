using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQOptionsFixture
    {
        protected RabbitMQOptions GetOptions()
        {
            var result = new RabbitMQOptions();

            new ConfigurationBuilder()
                .AddJsonFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @".\appsettings.json")).Build()
                .GetRequiredSection($"{RabbitMQOptions.SectionName}:local").Bind(result);

            return result;
        }

        [Test]
        public void Should_be_able_to_load_a_full_configuration()
        {
            var options = GetOptions();

            Assert.IsNotNull(options);

            Assert.AreEqual(TimeSpan.FromSeconds(30), options.RequestedHeartbeat);
            Assert.AreEqual(TimeSpan.FromMilliseconds(1500), options.QueueTimeout);
            Assert.AreEqual(TimeSpan.FromMilliseconds(1500), options.ConnectionCloseTimeout);
            Assert.AreEqual(5, options.OperationRetryCount);
            Assert.AreEqual(100, options.PrefetchCount);
        }
    }
}