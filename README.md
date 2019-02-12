# Rebus.Kafka

[![install from nuget](https://img.shields.io/nuget/v/Rebus.Kafka.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.Kafka)

##### Provides a [Apache Kafka](https://kafka.apache.org/) transport implementation for [Rebus](https://github.com/rebus-org/Rebus).
![](https://raw.githubusercontent.com/glazkovalex/Rebus.Kafka/master/image.png)
#### Using Rebus.Kafka:
```csharp
builder.RegisterRebus((configurer, context) => configurer
	.Transport(t => t.UseKafka("localhost:9092", "InputQueueName", "groupName"))
);
```
See unit tests for details.

#### Note: 
So as to interact with the Apache Kafka requires the unmanaged library "librdkafka", you need to install the package "[librdkafka.redist -Version 1.0.0-RC7](https://www.nuget.org/packages/librdkafka.redist/1.0.0-RC7 "librdkafka.redist -Version 1.0.0-RC7") or newer". If this unmanaged "librdkafka" is not found automatically, you must load it before you can use [Rebus.Kafka](https://github.com/glazkovalex/Rebus.Kafka) for the first time as follows:

```csharp
if (!Library.IsLoaded)
	Confluent.Kafka.Library.Load(pathToLibrd);
```
