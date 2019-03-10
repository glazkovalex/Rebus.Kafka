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

The parameters for the producer and the consumer can be specified in detail.

```csharp
var producerConfig = new ProducerConfig
{
	//BootstrapServers = , //will be set from the general parameter
	ApiVersionRequest = true,
	QueueBufferingMaxKbytes = 10240,
#if DEBUG
	Debug = "msg",
#endif
	MessageTimeoutMs = 3000,
};
producerConfig.Set("request.required.acks", "-1");
producerConfig.Set("queue.buffering.max.ms", "5");

var consumerConfig = new ConsumerConfig
{
	//BootstrapServers = , //will be set from the general parameter
	ApiVersionRequest = true,
	GroupId = "temp",
	EnableAutoCommit = false,
	FetchWaitMaxMs = 5,
	FetchErrorBackoffMs = 5,
	QueuedMinMessages = 1000,
	SessionTimeoutMs = 6000,
	//StatisticsIntervalMs = 5000,
#if DEBUG
	Debug = "msg",
#endif
	AutoOffsetReset = AutoOffsetReset.Latest,
	EnablePartitionEof = true
};
consumerConfig.Set("fetch.message.max.bytes", "10240");

Configure.With(adapter)
	.Transport(t => t.UseKafka("localhost:9092", "InputQueueName", producerConfig, consumerConfig))
	.Start();
```

See tests and examples for details.

### Note: 
- So as to interact with the Apache Kafka requires the unmanaged "librdkafka", you need to install the package "[librdkafka.redist -Version 1.0.0-RC7](https://www.nuget.org/packages/librdkafka.redist/1.0.0-RC7 "librdkafka.redist -Version 1.0.0-RC7") or newer". If this unmanaged "librdkafka" is not found automatically, you must load it before you can use [Rebus.Kafka](https://github.com/glazkovalex/Rebus.Kafka) for the first time as follows:

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
