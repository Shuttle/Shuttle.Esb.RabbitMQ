using System;
using System.IO;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQSectionFixture
    {
        protected RabbitMQSection GetRabbitMQSection(string file)
        {
            return ConfigurationSectionProvider.OpenFile< RabbitMQSection>("shuttle","rabbitmq", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, string.Format(@".\RabbitMQSection\files\{0}", file)));
        }
    }
}