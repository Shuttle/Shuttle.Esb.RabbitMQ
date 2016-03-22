using NUnit.Framework;

namespace Shuttle.Esb.RabbitMQ.Tests
{
	[TestFixture]
	public class RabbitMQSectionTest : RabbitMQSectionFixture
	{
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