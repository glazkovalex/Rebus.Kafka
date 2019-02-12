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
		// на основе : https://github.com/rebus-org/Rebus.AzureServiceBus/blob/master/Rebus.AzureServiceBus/Config/AzureServiceBusConfigurationExtensions.cs
		const string AsbSubStorageText = "The Kafka transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

		public static void UseKafka(this StandardConfigurer<ITransport> configurer,
			string brokerList, string inputQueueName, string groupId = null)
		{
			// Сопоставил транспорт самому себе внедрения в качестве ISubscriptionStorage
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

			// Совоставил ITransport к реализации даного транспорта
			configurer.Register(c => c.Get<KafkaTransport>());

			// Сопоставил хранилище подписчиков к самому транспорту
			configurer
				.OtherService<ISubscriptionStorage>()
				.Register(c => c.Get<KafkaTransport>(), description: AsbSubStorageText);
		}
	}
}
