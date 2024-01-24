using Confluent.Kafka;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Transport;
using System;
using Rebus.Kafka.Configs;

namespace Rebus.Kafka
{
	/// <summary>The Options of the Kafka transport</summary>
	public static class KafkaTransportOptions
	{
		// Based on: https://github.com/rebus-org/Rebus.AzureServiceBus/blob/master/Rebus.AzureServiceBus/Config/AzureServiceBusConfigurationExtensions.cs
		const string AsbSubStorageText = "The Kafka transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

		/// <summary>Configures Rebus to use Apache Kafka to transport messages. Performs a simplified
		/// configuration of the parameters of the producer and the consumer used in this transport.</summary>
		/// <param name="configurer"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
		/// <param name="inputQueueName">name of input queue</param>
		/// <param name="groupId">Id of group</param>
		public static void UseKafka(this StandardConfigurer<ITransport> configurer,
			string brokerList, string inputQueueName, string groupId = null)
		{
			// Register implementation of the transport as ISubscriptionStorage as well
			configurer
				.OtherService<KafkaTransport>()
				.Register(c =>
				{
					if (string.IsNullOrEmpty(inputQueueName))
						throw new ArgumentNullException(nameof(inputQueueName),
							$"You must supply a valid value for topicPrefix");
					var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
					var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory
						, brokerList, inputQueueName, groupId);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
		}

		/// <summary>Detailed Rebus configuration to use Apache Kafka to transport messages.
		/// Allows you to configure all the parameters of the producer and the consumer used in this transport.</summary>
		/// <param name="configurer"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.
		/// Overwrites 'bootstrap' values.server', possibly specified via producerConfig and consumerConfig</param>
		/// <param name="inputQueueName">name of input queue</param>
		/// <param name="producerConfig">A collection of librdkafka configuration parameters
		///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
		///     and parameters specific to this client (refer to:
		///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
		///     At a minimum, 'bootstrap.servers' must be specified.</param>
		/// <param name="consumerConfig">A collection of librdkafka configuration parameters
		///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
		///     and parameters specific to this client (refer to:
		///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
		///     At a minimum, 'bootstrap.servers' and 'group.id' must be
		///     specified.</param>
		public static void UseKafka(this StandardConfigurer<ITransport> configurer,
			string brokerList, string inputQueueName, ProducerConfig producerConfig, ConsumerConfig consumerConfig)
		{
			// Register implementation of the transport as ISubscriptionStorage as well
			configurer
				.OtherService<KafkaTransport>()
				.Register(c =>
				{
					if (string.IsNullOrEmpty(inputQueueName))
						throw new ArgumentNullException(nameof(inputQueueName),
							$"You must supply a valid value for topicPrefix");
					var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
					var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, brokerList
						, inputQueueName, producerConfig, consumerConfig);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
		}

		/// <summary>Detailed Rebus configuration to use Apache Kafka to transport messages.
		/// Allows you to configure all the parameters of the producer and the consumer used in this transport.</summary>
		/// <param name="configurer"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.
		/// Overwrites 'bootstrap' values.server', possibly specified via producerConfig and consumerConfig</param>
		/// <param name="inputQueueName">name of input queue</param>
		/// <param name="producerConfig">A collection of librdkafka configuration parameters
		///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
		///     and parameters specific to this client (refer to:
		///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
		///     At a minimum, 'bootstrap.servers' must be specified.</param>
        /// <param name="consumerAndBehaviorConfig">
        /// Contains behavior settings in the Behavior property in addition to a collection of librdkafka configuration parameters
        /// (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) and parameters specific to this client
        /// (refer to: <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        /// At a minimum, 'bootstrap.servers' and 'group.id' must be specified.
        /// </param>
		public static void UseKafka(this StandardConfigurer<ITransport> configurer,
			string brokerList, string inputQueueName, ProducerConfig producerConfig, ConsumerAndBehaviorConfig consumerAndBehaviorConfig)
		{
			// Register implementation of the transport as ISubscriptionStorage as well
			configurer
				.OtherService<KafkaTransport>()
				.Register(c =>
				{
					if (string.IsNullOrEmpty(inputQueueName))
						throw new ArgumentNullException(nameof(inputQueueName),
							$"You must supply a valid value for topicPrefix");
					var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
					var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, brokerList
						, inputQueueName, producerConfig, consumerAndBehaviorConfig);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
		}

		/// <summary>Configures Rebus to use Apache Kafka to transport messages as a one-way client (i.e. will not be able to receive any messages).
		/// Performs a simplified configuration of the parameters of the producer used in this transport.</summary>
		/// <param name="configurer"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
		public static void UseKafkaAsOneWayClient(this StandardConfigurer<ITransport> configurer, string brokerList)
		{
			// Register implementation of the transport as ISubscriptionStorage as well
			configurer
				.OtherService<KafkaTransport>()
				.Register(c =>
				{
					var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
					var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, brokerList, null, null);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);

			OneWayClientBackdoor.ConfigureOneWayClient(configurer);
		}

		/// <summary>Detailed Rebus configuration to use Apache Kafka to transport messages as a one-way client (i.e. will not be able to receive any messages).
		/// Allows you to configure all the parameters of the producerused in this transport.</summary>
		/// <param name="configurer"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.
		/// Overwrites 'bootstrap' values.server', possibly specified via producerConfig</param>
		/// <param name="producerConfig">A collection of librdkafka configuration parameters
		///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
		///     and parameters specific to this client (refer to:
		///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
		///     At a minimum, 'bootstrap.servers' must be specified.</param>
		public static void UseKafkaAsOneWayClient(this StandardConfigurer<ITransport> configurer,
			string brokerList, ProducerConfig producerConfig)
		{
			// Register implementation of the transport as ISubscriptionStorage as well
			configurer
				.OtherService<KafkaTransport>()
				.Register(c =>
				{
					var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
					var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, brokerList, null, producerConfig, null);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
			
			OneWayClientBackdoor.ConfigureOneWayClient(configurer);
		}
    }
}
