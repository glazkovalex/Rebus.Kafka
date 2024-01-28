using Autofac;
using Confluent.Kafka;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka;
using Scaleout.Messages;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Scaleout.Producer
{
    class Program
	{
		static async Task Main(string[] args)
		{
			var producerConfig = new ProducerConfig
			{
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
				ApiVersionRequest = true,
				//GroupId = // will be set random
				EnableAutoCommit = false,
				FetchWaitMaxMs = 5,
				FetchErrorBackoffMs = 5,
				QueuedMinMessages = 1000,
				SessionTimeoutMs = 6000,
				//StatisticsIntervalMs = 5000,
#if DEBUG
				TopicMetadataRefreshIntervalMs = 20000, // Otherwise it runs maybe five minutes
				Debug = "msg",
#endif
				AutoOffsetReset = AutoOffsetReset.Latest,
				EnablePartitionEof = true,
				AllowAutoCreateTopics = true
			};
			consumerConfig.Set("fetch.message.max.bytes", "10240");

			IContainer container;
			var builder = new ContainerBuilder();
			builder.RegisterInstance(new Counter(ItemCount)).As<Counter>().SingleInstance();
			builder.RegisterType<ConfirmationHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Confirmation)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Info))
				.Transport(t => t.UseKafka(_kafkaEndpoint, $"{typeof(Program).Namespace}.queue", producerConfig, consumerConfig))
                .Options(o => {
                    o.UseAttributeOrTypeFullNameForTopicNames();
                })
            );

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				await bus.Subscribe<Confirmation>();

				char key;
				do
				{
					var sw = Stopwatch.StartNew();
					var sendAmount = 0;
					var messages = Enumerable.Range(1, ItemCount)
						.Select(async i =>
						{
							sendAmount = sendAmount + i;
							await bus.Publish(new TestMessage { MessageNumber = i });
						}).ToArray();
					await Task.WhenAll(messages);
					Console.WriteLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}c");
					Console.WriteLine("Press any key to exit or 'r' to repeat.");
					key = Console.ReadKey().KeyChar;
				} while (key == 'r' || key == 'к');
			}
		}
		static readonly string _kafkaEndpoint = "confluent-kafka:9092";
		public static readonly int ItemCount = 50;
	}
}
