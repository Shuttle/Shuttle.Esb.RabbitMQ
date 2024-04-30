using RabbitMQ.Client;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.RabbitMQ
{
    public class ConfigureEventArgs
    {
        private ConnectionFactory _connectionFactory;

        public ConnectionFactory ConnectionFactory
        {
            get => _connectionFactory;
            set => _connectionFactory = value ?? throw new System.ArgumentNullException();
        }

        public ConfigureEventArgs(ConnectionFactory connectionFactory)
        {
            Guard.AgainstNull(connectionFactory, nameof(connectionFactory));

            _connectionFactory = connectionFactory;
        }
    }
}