using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Shuttle.Esb.RabbitMQ.Tests;

[TestFixture]
public class RabbitMQOptionsFixture
{
    protected RabbitMQOptions GetOptions()
    {
        var result = new RabbitMQOptions();

        new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @".\appsettings.json")).Build()
            .GetRequiredSection($"{RabbitMQOptions.SectionName}:local").Bind(result);

        return result;
    }

    [Test]
    public void Should_be_able_to_load_a_full_configuration()
    {
        var options = GetOptions();

        Assert.That(options, Is.Not.Null);

        Assert.That(options.RequestedHeartbeat, Is.EqualTo(TimeSpan.FromSeconds(30)));
        Assert.That(options.QueueTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(1500)));
        Assert.That(options.ConnectionCloseTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(1500)));
        Assert.That(options.OperationRetryCount, Is.EqualTo(5));
        Assert.That(options.PrefetchCount, Is.EqualTo(100));
    }
}