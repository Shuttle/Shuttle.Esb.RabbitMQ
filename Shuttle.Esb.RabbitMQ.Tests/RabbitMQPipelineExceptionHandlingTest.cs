using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
	public class RabbitMQPipelineExceptionHandlingTest : PipelineExceptionFixture
	{
		[Test]
		public void Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline()
		{
			var queueUriFormat = "rabbitmq://shuttle:shuttle!@localhost/{0}";
			TestExceptionHandling(queueUriFormat);
		}
	}
}