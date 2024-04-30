# RabbitMQ Docker

```
docker run -d --name rabbitmq -p 15672:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=shuttle -e RABBITMQ_DEFAULT_PASS=shuttle! rabbitmq:3-management
```