using Autofac;
using Confluent.Kafka;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka;
using Rebus.Routing.TypeBased;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace BusScaleoutWitchDetailedConfiguration
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

			const int ITEM_COUNT = 100;

			IContainer container;
			var builder = new ContainerBuilder();
			builder.RegisterInstance(new Counter(ITEM_COUNT)).As<Counter>().SingleInstance();
			builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Info))
				.Transport(t => t.UseKafka(_kafkaEndpoint
					, nameof(BusScaleoutWitchDetailedConfiguration), producerConfig, consumerConfig))
				.Routing(r => r.TypeBased().Map<Message>(nameof(BusScaleoutWitchDetailedConfiguration)))
				.Options(o => o.SetMaxParallelism(4))
			);

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				bus.Subscribe<Message>().Wait();
				Task.Delay(5000).Wait(); // for wait complete rebalance
				var sw = Stopwatch.StartNew();
				var sendAmount = 0;
				var messages = Enumerable.Range(1, ITEM_COUNT)
					.Select(i =>
					{
						sendAmount = sendAmount + i;
						return bus.Publish(new Message { MessageNumber = i });
					}).ToArray();
				Task.WaitAll(messages);
				Console.WriteLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}c");
				Console.WriteLine("Press any key to exit.");
				Console.ReadKey();
			}
		}
		static readonly string _kafkaEndpoint = "192.168.0.166:9092";
	}
}
