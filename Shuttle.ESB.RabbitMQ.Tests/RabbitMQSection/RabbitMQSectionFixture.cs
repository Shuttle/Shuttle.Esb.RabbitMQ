namespace Shuttle.ESB.RabbitMQ.Tests
{
	public class RabbitMQSectionFixture
	{
		protected RabbitMQSection GetRabbitMQSection(string file)
		{
			return RabbitMQSection.Open(string.Format(@".\RabbitMQSection\files\{0}", file));
		}
	}
}