using Shuttle.Core.Infrastructure;

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