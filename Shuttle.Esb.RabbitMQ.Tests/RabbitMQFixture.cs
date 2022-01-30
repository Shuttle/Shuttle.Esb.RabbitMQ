using Castle.Windsor;
using Shuttle.Core.Castle;
using Shuttle.Core.Container;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.RabbitMQ.Tests
{
    public static class RabbitMQFixture
    {
        public static ComponentContainer GetComponentContainer()
        {
            var container = new WindsorComponentContainer(new WindsorContainer());

            container.Register<IRabbitMQConfiguration, RabbitMQConfiguration>();

            return new ComponentContainer(container, () => container);
        }
    }
}