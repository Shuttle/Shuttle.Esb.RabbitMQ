using System;
using System.IO;

namespace Shuttle.ESB.RabbitMQ.Tests
{
    public class RabbitMQSectionFixture
    {
        protected RabbitMQSection GetRabbitMQSection(string file)
        {
            return RabbitMQSection.Open(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, string.Format(@".\RabbitMQSection\files\{0}", file)));
        }
    }
}