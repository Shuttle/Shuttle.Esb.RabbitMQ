using Shuttle.Core.Container;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
	public class Bootstrap : IComponentRegistryBootstrap
	{
		public void Register(IComponentRegistry registry)
		{
			Guard.AgainstNull(registry, "registry");

			if (!registry.IsRegistered<IRabbitMQConfiguration>())
			{
				registry.Register<IRabbitMQConfiguration, RabbitMQConfiguration>();
			}
		}
	}
}