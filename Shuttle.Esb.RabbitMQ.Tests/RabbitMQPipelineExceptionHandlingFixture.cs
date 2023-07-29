using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public class RabbitMQPipelineExceptionHandlingFixture : PipelineExceptionFixture
    {
        [Test]
        public async Task Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline()
        {
            await TestExceptionHandling(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
        }
    }
}