using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests;

public class RabbitMQPipelineExceptionHandlingFixture : PipelineExceptionFixture
{
    [Test]
    public async Task Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline_async()
    {
        await TestExceptionHandlingAsync(RabbitMQFixture.GetServiceCollection(), "rabbitmq://local/{0}");
    }
}