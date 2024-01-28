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

See [examples](https://github.com/glazkovalex/Rebus.Kafka/tree/master/Examples) and [tests](https://github.com/glazkovalex/Rebus.Kafka/tree/master/Rebus.Kafka.Tests) for other usage examples.

### This provider supports the following Rebus bus functions:

* Additional Apache Kafka transport. The tests use an [confluentinc/cp-kafka](https://hub.docker.com/r/confluentinc/cp-kafka) container.
* [Routing](https://github.com/rebus-org/Rebus/wiki/Routing)
* [Pub sub messaging](https://github.com/rebus-org/Rebus/wiki/Pub-sub-messaging)
* [Process managers (sagas)](https://github.com/rebus-org/RebusSamples/blob/master/Sagas/README.md). See [example](https://github.com/glazkovalex/Rebus.Kafka/blob/14e06b88634b9b2b1051026ed808b7f1148bab52/Examples/IdempotentSaga/Program.cs#L49).
* [Workers and parallelism](https://github.com/rebus-org/Rebus/wiki/Workers-and-parallelism)
* [Automatic retries and error handling](https://github.com/rebus-org/Rebus/wiki/Automatic-retries-and-error-handling)
* ["Transaction"](https://github.com/rebus-org/Rebus/wiki/Transactions) 
* [Headers](https://github.com/glazkovalex/Rebus.Kafka/blob/d7297c278bc5ecf9181e48d443950073dd5fd7ed/Rebus.Kafka.Tests/SimpleTests.cs#L38)
* [Timeouts](https://github.com/rebus-org/Rebus/wiki/Timeouts)

Many others are probably supported too, but I haven't checked.

### Additional features:

* When using the Rebus package.Service Provider, to avoid deadlock, events should be subscribed `await bus.Subscribe<TestMessage>()` to only after the bus is started, but NOT in the "onCreate" rebus event!
* At the request of the transport users, the [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) automatically creates topics by default. However, I do not recommend using allow.auto.create.topics=true for production! To disable allow.auto.create.topics, pass your ConsumerConfig or ConsumerAndBehaviorConfig configuration to the transport with the AllowAutoCreateTopics = false parameter disabled.
* The [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) has a new overload with the [ConsumerAndBehaviorConfig](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerAndBehaviorConfig.cs) parameter instead of ConsumerConfig. This new configuration type contains transport behavior settings. So far, it has a single [CommitPeriod](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerBehaviorConfig.cs) parameter that defines the period after which the commit offset will be set in Apache Kafka. [Here is an example of using it ](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka.Tests/SimpleTests.cs#L69)
* [UseAttributeOrTypeFullNameForTopicNames](https://github.com/glazkovalex/Rebus.Kafka/blob/e292ac48d9049785123bb2ab005969ed0c3ccb48/IdempotentSaga/Program.cs#L58) simplifies naming the topic of events by Name from [TopicAttribute({TopicName})](https://github.com/glazkovalex/Rebus.Kafka/blob/e292ac48d9049785123bb2ab005969ed0c3ccb48/IdempotentSaga/Messages/KickoffSagaMessages.cs#L5) or to "---Topic---.{Spacename}.{TypeName}".
* [KafkaAdmin](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/KafkaAdmin.cs) which allows you to programmatically create a topic with many partitions and delete topics.
 
### Note: 
- So as to interact with the Apache Kafka requires the unmanaged "librdkafka", you need to install the appropriate version of the package "[librdkafka.redist](https://www.nuget.org/packages/librdkafka.redist)". If this unmanaged "librdkafka" is not found automatically, you must load it before you can use [Rebus.Kafka](https://github.com/glazkovalex/Rebus.Kafka) for the first time as follows:

```csharp
if (!Library.IsLoaded)
	Confluent.Kafka.Library.Load(pathToLibrd);
```

- Due to the features of Apache Kafka, after subscribing or unsubscribing to messages for some time while there is **very slowly rebalancing** of clients in groups, lasting several seconds or more. therefore, you should avoid the scenario of dynamic subscription to a single reply message, sending a single message to the recipient, and unsubscribing from the message after receiving a single reply. Since this scenario will work very slowly. I recommend that you subscribe to all your messages only when the application starts and that you do not change subscribers in runtime, then the work of transport will be fast.

### Log of important changes:
#### V 3.0.1 (12.01.2024)
1. Refactoring for Rebus version 8 with the corresponding API change;
2. Implemented RetryStrategy - [automatic retries and error handling](https://github.com/rebus-org/Rebus/wiki/Automatic-retries-and-error-handling). Confirmations of receipt of messages are now sent not after they are received, but only after successful processing of messages or sending them to the error topic; 
3. Add ["transaction"](https://github.com/rebus-org/Rebus/wiki/Transactions) support. More precisely, not transactions, because Apache Kafka does not support transactions, but delayed sending of all transaction messages before calling [await scope.Complete Async()](https://github.com/glazkovalex/Rebus.Kafka/blob/bb7775d2b395fac10d2840517649722e279115e0/Rebus.Kafka.Tests/TransactionsTests.cs#L69) or canceling the sending of all "sent" messages at the end of the transaction block without calling await scope.Complete Async(). This convenience slows down the maximum performance of sending all messages by half, even those messages that are sent without transactions.

#### V 2.0.0 (18.08.2023)
1. Improving data transfer efficiency; 
2. The format of transport messages has changed. In them now the key is not Null, but string. The messages are incompatible with previous versions of the transport!
3. Message headers are now supported;
4. Refactoring for the current version of Apache Kafka "confluentinc/cp-kafka:7.0.1"; 
5. Transport forcibly creates missing topics if Consumer.Config.AllowAutoCreateTopics == true; However, I do not recommend using allow.auto.create.topics=true for production! 

#### V 1.6.3 (1.04.2021)
1. The [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) has a new overload with the [ConsumerAndBehaviorConfig](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerAndBehaviorConfig.cs) parameter instead of ConsumerConfig. This new configuration type contains transport behavior settings. So far, it has a single [CommitPeriod](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka/Configs/ConsumerBehaviorConfig.cs) parameter that defines the period after which the commit offset will be set in Apache Kafka. [Here is an example of using it ](https://github.com/glazkovalex/Rebus.Kafka/blob/master/Rebus.Kafka.Tests/SimpleTests.cs#L69)

2. In the summer of 2020, the [Librdkafka v1.5.0 library was updated, which was a change unexpected](https://github.com/edenhill/librdkafka/releases/tag/v1.5.0) for many users of the [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka). 
	> Consumer will no longer trigger auto creation of topics, allow.auto.create.topics=true may be used to re-enable the old deprecated functionality:
 
	At the request of the transport users, I enabled the previous transport behavior by default. **Now the [Rebus.Kafka transport](https://github.com/glazkovalex/Rebus.Kafka) automatically creates topics by default as before**. 
	However, I do not recommend using allow.auto.create.topics=true for production! To disable allow.auto.create.topics, pass your ConsumerConfig or ConsumerAndBehaviorConfig configuration to the transport with the AllowAutoCreateTopics = false parameter disabled.

### ToDo:
- Schema Registry support in Kafka: Avro, JSON and Protobuf
- In the future, the value from the message header "kafka-key" or, maybe, from the message property marked with the KafkaKey attribute will be inserted into the Apache Kafka message key. This will be useful for partitioning.
- Start the transport from user-defined offsets for topics and partitions.
---
If you have any recommendations or comments, I will be glad to hear.
