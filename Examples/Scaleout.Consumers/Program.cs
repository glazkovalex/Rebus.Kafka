using Autofac;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka;
using Scaleout.Messages;
using System;
using System.Threading.Tasks;

namespace Scaleout.Consumers
{
    public class Program
	{
		static async Task Main(string[] args)
		{
			IContainer container;
			var builder = new ContainerBuilder();
			builder.RegisterType<TestMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(TestMessage)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Info))
				.Transport(t => t.UseKafka(_kafkaEndpoint, $"{typeof(Program).Namespace}.queue", "commonGroupForScaleout"))
				.Options(o => {
					o.SetMaxParallelism(2);
                    o.UseAttributeOrTypeFullNameForTopicNames();
                })
			);

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				await bus.Subscribe<TestMessage>();
				Console.WriteLine($"If you have created a topic \"---Topic---.Scaleout.Messages.TestMessage\" with two partitions, then run a second consumer to process them twice as fast");
				Console.WriteLine("Waiting for messages. Press any key to exit.");
				Console.ReadKey();
			}
		}
		static readonly string _kafkaEndpoint = "confluent-kafka:9092";
	}
}
