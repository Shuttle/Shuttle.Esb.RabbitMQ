using Microsoft.Extensions.Options;

namespace Shuttle.Esb.RabbitMQ;

public class RabbitMQOptionsValidator : IValidateOptions<RabbitMQOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMQOptions options)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return ValidateOptionsResult.Fail(Esb.Resources.QueueConfigurationNameException);
        }

        if (string.IsNullOrWhiteSpace(options.Host))
        {
            return ValidateOptionsResult.Fail(string.Format(Esb.Resources.QueueConfigurationItemException, name, nameof(options.Host)));
        }

        return ValidateOptionsResult.Success;
    }
}