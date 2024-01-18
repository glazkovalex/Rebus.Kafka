using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rebus.Kafka;
using Serilog;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
            var globalScope = ConfigureServices();
            var loggerFactory = globalScope.ServiceProvider.GetRequiredService<ILoggerFactory>();
            ILogger<KafkaProducer<Null, string>> loggerProducer = loggerFactory.CreateLogger<KafkaProducer<Null, string>>();
            var loggerConsumer = loggerFactory.CreateLogger<KafkaConsumer<Null, string>>();

            using (var dependentKafkaProducer = new KafkaProducer<Null, string>(_kafkaEndpoint, loggerProducer))
            using (var producer = new KafkaProducer<Null, string>(dependentKafkaProducer)) // for test dependentKafkaProducer 
            using (KafkaConsumer<Null, string> consumer = new KafkaConsumer<Null, string>(_kafkaEndpoint, loggerConsumer)
                , consumer2 = new KafkaConsumer<Null, string>(_kafkaEndpoint, loggerConsumer))
            {
                consumer.Consume(new[] { bTopicNameResp })
                    .Subscribe(result =>
                    {
                        Console.WriteLine($"Boy name {result.Message.Value} is recommended. (test-header:{(result.Message.Headers.TryGetLastBytes("test-header", out byte[] data) ? data[0].ToString() : "null")})");
                        consumer.Commit(result.TopicPartitionOffset);
                    }, cts.Token);
                consumer2.Consume(new[] { gTopicNameResp })
                    .Subscribe(result => {
                        Console.WriteLine($"Girl name {result.Message.Value} is recommended. (test-header:{(result.Message.Headers.TryGetLastBytes("test-header", out byte[] data) ? data[0].ToString() : "null")})");
                        consumer2.Commit(result.TopicPartitionOffset);
                    }, cts.Token);

                string userInput;
                var rnd = new Random();
                do
                {
                    Console.WriteLine(userHelpMsg);
                    userInput = Console.ReadLine();
                    switch (userInput)
                    {
                        case "b":
                            var nameCount = 1000;

                            Task[] jobs = Enumerable.Range(0, nameCount)
                                .Select(i => new Message<Null, string>
                                {
                                    Value = $"{i:D4} {_boyNames[rnd.Next(0, 5)]}",
                                    Headers = new Headers { { "test-header", new byte[] { (byte)rnd.Next(0, 0xFF) } } }
                                })
                                .Select(m => producer.ProduceAsync(bTopicNameResp, m))
                                .ToArray();
                            Stopwatch sw = Stopwatch.StartNew();
                            Task.WaitAll(jobs);
                            Console.WriteLine($"Sending {nameCount} за {sw.ElapsedMilliseconds / 1000:N3}с");
                            break;
                        case "g":
                            producer.ProduceAsync(gTopicNameResp, new Message<Null, string>
                            {
                                Value = _girlNames[rnd.Next(0, 5)],
                                Headers = new Headers { { "test-header", new byte[] { (byte)rnd.Next(0, 0xFF) } } }
                            }, cts.Token).GetAwaiter().GetResult();
                            break;
                        case "q":
                            cts.Cancel();
                            Thread.Sleep(1000);
                            break;
                        default:
                            Console.WriteLine($"Unknown command.");
                            break;
                    }
                } while (userInput != "q");
            }
        }

        private static IServiceScope ConfigureServices()
        {
            var logger = Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            var services = new ServiceCollection();
            services.AddLogging(builder => builder.AddSerilog(logger, true));

            return services.BuildServiceProvider().CreateScope();
        }

        static readonly string _kafkaEndpoint = "confluent-kafka:9092";
        private static readonly string bTopicNameResp = "b_name_response";
        private static readonly string gTopicNameResp = "g_name_response";
        private static readonly string[] _boyNames =
        {
            "Arsenii",
            "Igor",
            "Kostya",
            "Ivan",
            "Dmitrii",
        };

        private static readonly string[] _girlNames =
        {
            "Nastya",
            "Lena",
            "Ksusha",
            "Katya",
            "Olga"
        };
        private static readonly string userHelpMsg =
            "Enter 'b' or 'g' to process boy or girl names respectively. Press q to exit.";
    }
}
