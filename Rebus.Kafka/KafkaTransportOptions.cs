using System;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.Kafka
{
	public static class KafkaTransportOptions
	{
		// Based on: https://github.com/rebus-org/Rebus.AzureServiceBus/blob/master/Rebus.AzureServiceBus/Config/AzureServiceBusConfigurationExtensions.cs
		const string AsbSubStorageText = "The Kafka transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

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
					return new KafkaTransport(rebusLoggerFactory, asyncTaskFactory, brokerList, inputQueueName, groupId);
				});

			// Register implementation of the Transport as ITransport
			configurer.Register(c => c.Get<KafkaTransport>());

			// Link the ISubscriberStorage to the transport
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
		}
	}
}
