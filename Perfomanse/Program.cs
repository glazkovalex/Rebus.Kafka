﻿using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rebus.Kafka;
using Serilog;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Perfomance
{
    class Program
    {
        static async Task Main(string[] args)
        {
            int perfomanceCount = 10000;
            string perfomanceCountText = perfomanceCount.ToString() + "perfomanceCount.ToString()";
            string topic = "ConfluentPerformance";

            CancellationTokenSource cts = new CancellationTokenSource();
            //var producerLogger = new TestLogger<KafkaProducer>(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Info }.GetLogger<KafkaProducer>());
            using (var producer = new KafkaProducer(_kafkaEndpoint/*, producerLogger*/))
            {
                //var consumerLogger = new TestLogger<KafkaConsumer>(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Info }.GetLogger<KafkaConsumer>());
                Stopwatch swHandle = null;
                KafkaConsumer consumer = new KafkaConsumer(_kafkaEndpoint, "temp"/*, consumerLogger*/);
                {
                    var obs = consumer.Consume(new[] { topic });
                    obs.Subscribe(message =>
                    {
                        swHandle ??= Stopwatch.StartNew();
                        if (message.Value == perfomanceCountText)
                        {
                            swHandle.Stop();
                            Console.WriteLine($"Confluent received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                            //cts.Cancel();
                        }
                    }, cts.Token);
                    await Task.Delay(5000);
                    Stopwatch sw = Stopwatch.StartNew();
                    var jobs = Enumerable.Range(1, perfomanceCount)
                        .Select(i => new Message<Null, string> { Value = i.ToString() + "perfomanceCount.ToString()" })
                        .Select(m => producer.ProduceAsync(topic, m));
                    await Task.WhenAll(jobs);
                    Console.WriteLine($"Confluent send {perfomanceCount} in {sw.ElapsedMilliseconds / 1000f:N3}с");

                    await Task.Delay(5000);
                    //cts.Cancel();
                    await Task.Delay(100);
                }
            }

            return;

            //CancellationTokenSource cts = new CancellationTokenSource();
			Console.CancelKeyPress += (_, e) =>
			{
				e.Cancel = true; // prevent the process from terminating.
				cts.Cancel();
			};
			var globalScope = ConfigureServices();
			var loggerFactory = globalScope.ServiceProvider.GetRequiredService<ILoggerFactory>();
			ILogger<KafkaProducer> loggerProducer = loggerFactory.CreateLogger<KafkaProducer>();
			var loggerConsumer = loggerFactory.CreateLogger<KafkaConsumer>();

			using (var dependentKafkaProducer = new KafkaProducer(_kafkaEndpoint, loggerProducer))
			using (var producer = new KafkaProducer(dependentKafkaProducer)) // for test dependentKafkaProducer 
			using (KafkaConsumer consumer = new KafkaConsumer(_kafkaEndpoint, loggerConsumer)
				, consumer2 = new KafkaConsumer(_kafkaEndpoint, loggerConsumer))
			{
				consumer.Consume(new[] { bTopicNameResp })
					.Subscribe(message => Console.WriteLine($"Boy name {message.Value} is recommended"), cts.Token);
				consumer2.Consume(new[] { gTopicNameResp })
					.Subscribe(message => Console.WriteLine($"Girl name {message.Value} is recommended"), cts.Token);

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
								.Select(i => new Message<Null, string> { Value = $"{i:D4} {_boyNames[rnd.Next(0, 5)]}" })
								.Select(m => producer.ProduceAsync(bTopicNameResp, m))
								.ToArray();
							Stopwatch sw = Stopwatch.StartNew();
							Task.WaitAll(jobs);
							Console.WriteLine($"Sending {nameCount} за {sw.ElapsedMilliseconds / 1000:N3}с");
							break;
						case "g":
							producer.ProduceAsync(gTopicNameResp, new Message<Null, string> { Value = _girlNames[rnd.Next(0, 5)] }
								, cts.Token).GetAwaiter().GetResult();
							break;
						case "q":
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

        static readonly string _kafkaEndpoint = "127.0.0.1:9092";
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
