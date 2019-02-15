using Autofac;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka;
using Scaleout.Messages;
using System;

namespace Scaleout.Consumers
{
	public class Program
	{
		static void Main(string[] args)
		{
			IContainer container;
			var builder = new ContainerBuilder();
			builder.RegisterType<TestMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(TestMessage)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Info))
				.Transport(t => t.UseKafka(_kafkaEndpoint
					, "scaleout.consumers", "commonGroupForScaleout"))
				.Options(o => o.SetMaxParallelism(2))
			);

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				bus.Subscribe<TestMessage>().Wait();
				Console.WriteLine($"If your Kafka \"num.partitions\" > 1 to start the second instance of \"Scaleout.Consumers\"");
				Console.WriteLine("Waiting for messages. Press any key to exit.");
				Console.ReadKey();
				bus.Unsubscribe<TestMessage>().Wait(); // only for test
			}
		}
		static readonly string _kafkaEndpoint = "192.168.0.166:9092";
	}
}
