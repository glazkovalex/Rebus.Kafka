using Autofac;
using Confluent.Kafka;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka;
using Rebus.Routing.TypeBased;
using Scaleout.Messages;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Scaleout.Producer
{
	class Program
	{
		static void Main(string[] args)
		{
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
				.Transport(t => t.UseKafka(_kafkaEndpoint, "scaleout.producer", producerConfig, consumerConfig))
				//.Routing(r => r.TypeBased().MapAssemblyOf<TestMessage>("scaleout.consumers"))
			);

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				bus.Subscribe<Confirmation>().Wait();

				char key;
				do
				{
					var sw = Stopwatch.StartNew();
					var sendAmount = 0;
					var messages = Enumerable.Range(1, ItemCount)
						.Select(i =>
						{
							sendAmount = sendAmount + i;
							return bus.Publish(new TestMessage { MessageNumber = i });
						}).Concat(Enumerable.Range(1, ItemCount)
                        .Select(i =>
                        {
							int value = i + 10;
                            sendAmount = sendAmount + value;
                            return bus.Publish(new TestMessage2 { MessageNumber = value });
                        })
                    ).ToArray();
                    Task.WaitAll(messages);
					Console.WriteLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}c");
					Console.WriteLine("Press any key to exit or 'r' to repeat.");
					key = Console.ReadKey().KeyChar;
				} while (key == 'r' || key == 'к');

				bus.Unsubscribe<Confirmation>().Wait(); // only for test
			}
		}
		static readonly string _kafkaEndpoint = "confluent-kafka:9092";
		public static readonly int ItemCount = 10;
	}
}
