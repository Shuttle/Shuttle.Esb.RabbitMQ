using System;
using NUnit.Framework;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    [TestFixture]
    public class RabbitMQUriParserFixture
    {
        [Test]
        public void Should_be_able_to_parse_all_parameters()
        {
            var parser =
                new RabbitMQUriParser(
                    new Uri("rabbitmq://my-user:my-pwd@my-host:100/my-vhost/my-queue?prefetchCount=250&durable=false&persistent=false"));

            Assert.AreEqual("my-user", parser.Username);
            Assert.AreEqual("my-pwd", parser.Password);
            Assert.AreEqual("my-host", parser.Host);
            Assert.AreEqual(100, parser.Port);
            Assert.AreEqual("my-vhost/", parser.VirtualHost);
            Assert.AreEqual(250, parser.PrefetchCount);
            Assert.AreEqual(false, parser.Durable);
            Assert.AreEqual(false, parser.Persistent);
        }
    }
}