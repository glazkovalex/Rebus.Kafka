using TwoBusInHost.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Kafka;
using Rebus.Kafka.Configs;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using Rebus.ServiceProvider;
using Rebus.Transport.InMem;
using Scaleout.Messages;
using System.Diagnostics;
using Rebus.Threading;

namespace TwoBusInHost
{
    class Program
    {
        //static InMemNetwork network = new InMemNetwork();
        static async Task Main(string[] args)
        {
            throw new NotImplementedException("It's not working yet");
            using (var host = CreateHostBuilder(args).Build())
            {
                var bus = host.Services.GetRequiredService<IBus>();
                var busRegistry = host.Services.GetRequiredService<IBusRegistry>();
                IBus busOneWay = busRegistry.GetBus("OneWayBus");
                char key;
                do
                {
                    var sendAmount = 0;
                    sw.Restart();
                    var messages = Enumerable.Range(1, MessageCount)
                        .Select(async i =>
                        {
                            Interlocked.Add(ref sendAmount, i);
                            await bus.Send(new TestMessage { MessageNumber = i });
                        }).ToArray();
                    Task.WaitAll(messages);
                    Console.WriteLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}c");
                    Console.WriteLine("Press any key to exit or 'r' to repeat.");
                    key = Console.ReadKey().KeyChar;
                } while (key == 'r' || key == 'к');
                await host.StopAsync();
            }
            Console.ReadKey();
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args);
            builder.ConfigureServices((hostContext, services) =>
            {
                var consoleLoggerFactory = new ConsoleLoggerFactory(true) { MinLevel = LogLevel.Debug };
                var queue = $"{nameof(TwoBusInHost)}.queue";
                services.AddSingleton(consoleLoggerFactory);
                services.AddRebusHandler<TestMessageHandler>();
                services.AddRebus((configurer, serviceProvider) => configurer
                    .Logging(l => l.Use(consoleLoggerFactory))
                    .Transport(t => t.UseKafka(kafkaEndpoint, queue, "temp"))
                    //.Transport( t=> t.UseInMemoryTransport(network, queue))
                    .Routing(r => r.TypeBased().Map<TestMessage>(queue))
                    .Options(o =>
                    {
                        o.SetNumberOfWorkers(10);
                        o.SetMaxParallelism(10);
                    }));
                services.AddRebus((configurer, serviceProvider) => configurer
                    .Logging(l => l.Use(consoleLoggerFactory))
                    .Transport(t => t.UseKafkaAsOneWayClient(kafkaEndpoint))
                    //.Transport(t => t.UseInMemoryTransportAsOneWayClient(network))
                    , isDefaultBus: false, key: "OneWayBus");
            });
            return builder;
        }

        internal const int MessageCount = 1;
        internal static Stopwatch sw = Stopwatch.StartNew();
        static readonly string kafkaEndpoint = "confluent-kafka:9092";
    }
}
