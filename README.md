# RabbitMQQueue

This RabbitMQ implementation follows the `at-least-once` delivery mechanism supported by Shuttle.Esb and since RabbitMQ is a broker all communication takes place immediately with the broker.

If necessary you may want to use an *outbox* for a `store-and-forward` solution.  By using a transactional outbox such as the sql implementation you could roll back sending of messages on failure.

## Installation

If you need to install RabbitMQ you can <a target='_blank' href='https://www.rabbitmq.com/install-windows.html'>follow these instructions</a>.

## Configuration

The queue configuration is part of the specified uri, e.g.:

``` xml
<inbox
    workQueueUri="rabbitmq://username:password@host:port/virtualhost/queue?prefetchCount=25&amp;durable=true&amp;persistent=true"
    .
    .
    .
/>
```

| Segment / Argument | Default    | Description | Version Introduced |
| --- | --- | --- | --- |
| username:password     | empty|    | |
| virtualhost         | /    |    | |
| port                 | default    |    | |
| prefetchcount             | 25        | Specifies the number of messages to prefetch from the queue. | |
| durable             | true     | Determines whether the queue is durable.  Note: be very mindful of the possible consequences before setting to 'false'. | v3.5.0 |
| persistent             | true     | Determines whether messages will be persisted.  Note: be very mindful of the possible consequences before setting to 'false'. | v3.5.3 |
| priority             | empty     | Determines the number of priorities supported by the queue. | v10.0.10 |

In addition to this there is also a RabbitMQ specific section (defaults specified here):

``` xml
<configuration>
  <configSections>
    <section name='rabbitmq' type="Shuttle.Esb.RabbitMQ.RabbitMQSection, Shuttle.Esb.RabbitMQ"/>
  </configSections>
  
  <rabbitmq
    localQueueTimeoutMilliseconds="250"
    remoteQueueTimeoutMilliseconds="2000"
    connectionCloseTimeoutMilliseconds="1000"
    requestedHeartbeat="30"
    operationRetryCount="3"
    useBackgroundThreadsForIO="true"
  />
  .
  .
  .
<configuration>
```

A `RabbitMQConfiguration` instance implementing the `IRabbitMQConfiguration` interface will be registered using the [container bootstrapping](http://shuttle.github.io/shuttle-core/overview-container/#Bootstrapping).  If you wish to override the configuration you should register your instance before calling the `ServiceBus.Register()` method.
