# Rebus.Kafka

[![install from nuget](https://img.shields.io/nuget/v/Rebus.Kafka.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.Kafka)

##### Provides a [Apache Kafka](https://kafka.apache.org/) transport implementation for [Rebus](https://github.com/rebus-org/Rebus).
![](https://raw.githubusercontent.com/glazkovalex/Rebus.Kafka/master/image.png)
### Getting started Rebus.Kafka:
1. Implement [Getting started for Rebus](https://github.com/rebus-org/Rebus#getting-started)
2. Add transport as UseKafka
```csharp
builder.RegisterRebus((configurer, context) => configurer
	.Transport(t => t.UseKafka("localhost:9092", "InputQueueName", "groupName"))
);
```

All parameters for the producer and the consumer can be specified in detail. [See this example](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Examples/Scaleout.Producer/Program.cs).

It is possible to configures Rebus to use Apache Kafka to transport messages as a one-way client (i.e. will not be able to receive any messages). [See this example](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Examples/KafkaAsOneWayTransport/Program.cs).

```csharp
builder.RegisterRebus((configurer, context) => configurer
	.Transport(t => t.UseKafkaAsOneWayClient("localhost:9092"))
);
```

See examples and tests for other usage examples.

### Note: 
- So as to interact with the Apache Kafka requires the unmanaged "librdkafka", you need to install the appropriate version of the package "[librdkafka.redist](https://www.nuget.org/packages/librdkafka.redist)". If this unmanaged "librdkafka" is not found automatically, you must load it before you can use [Rebus.Kafka](https://github.com/glazkovalex/Rebus.Kafka) for the first time as follows:

```csharp
if (!Library.IsLoaded)
	Confluent.Kafka.Library.Load(pathToLibrd);
```

- Due to the features of Apache Kafka, after subscribing or unsubscribing to messages for some time while there is **very slowly rebalancing** of clients in groups, lasting several seconds or more. therefore, you should avoid the scenario of dynamic subscription to a single reply message, sending a single message to the recipient, and unsubscribing from the message after receiving a single reply. Since this scenario will work very slowly. I recommend that you subscribe to all your messages only when the application starts and that you do not change subscribers in runtime, then the work of transport will be fast.

### ToDo:
- Add configures Rebus to use Apache Kafka to transport messages with two consumers. One in the group for scalable processing of messages from the incoming queue, and the other in the unique group for receiving integration events.
- Add transaction support.

---
If you have any recommendations or comments, I will be glad to hear.
