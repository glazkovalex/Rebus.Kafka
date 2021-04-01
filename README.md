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

### Log of important changes:
#### V 1.6.3 (1.04.2021)
1. The [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) has a new overload with the [ConsumerAndBehaviorConfig](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerAndBehaviorConfig.cs) parameter instead of ConsumerConfig. This new configuration type contains transport behavior settings. So far, it has a single [CommitPeriod](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerBehaviorConfig.cs) parameter that defines the period after which the commit offset will be set in Apache Kafka. [Here is an example of using it ](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka.Tests/SimpleTests.cs#L69)

2. In the summer of 2020, the [Librdkafka v1.5.0 library was updated, which was a change unexpected](https://github.com/edenhill/librdkafka/releases/tag/v1.5.0) for many users of the [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka). 
	> Consumer will no longer trigger auto creation of topics, allow.auto.create.topics=true may be used to re-enable the old deprecated functionality:
 
	At the request of the transport users, I enabled the previous transport behavior by default. **Now the [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) automatically creates topics by default as before**. 
	However, I do not recommend using allow.auto.create.topics=true for production! To disable allow.auto.create.topics, pass your ConsumerConfig or ConsumerAndBehaviorConfig configuration to the transport with the AllowAutoCreateTopics = false parameter disabled.

### ToDo:
- Add SimpleRetryStrategy support.
- Add transaction support.

---
If you have any recommendations or comments, I will be glad to hear.
