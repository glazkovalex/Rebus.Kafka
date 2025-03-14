﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Kafka;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using Rebus.ServiceProvider;
using System.Diagnostics;
using SchemaRegistry.Events;
using Confluent.SchemaRegistry;
using SchemaRegistry.Messages;
using SchemaRegistry.Interceptors;
using SchemaRegistry.Handlers;

namespace SchemaRegistry
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            host.RunAsync();
            var busRegistry = host.Services.GetRequiredService<IBusRegistry>();
            IBus defaultBus = host.Services.GetRequiredService<IBus>();
            await defaultBus.Subscribe<SimpleEvent>();
            IBus oneWayBus = busRegistry.GetBus("OneWayBus");

            char key;
            do
            {
                var sendAmount = 0;
                sw.Restart();
                var messages = Enumerable.Range(1, MessageCount)
                    .Select(async i =>
                    {
                        Interlocked.Add(ref sendAmount, i);
                        await oneWayBus.Send(new TestMessage { MessageNumber = i });
                        await oneWayBus.Publish(new SimpleEvent { Id = Guid.NewGuid(), EventNumber = i });
                    }).ToArray();
                Task.WaitAll(messages);
                Console.WriteLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}c");
                Console.WriteLine("Press any key to exit or 'r' to repeat.");

                key = Console.ReadKey().KeyChar;
            } while (key == 'r' || key == 'к');
            await host.StopAsync();
            Console.ReadKey();
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args);
            builder.ConfigureServices((hostContext, services) =>
            {
                var consoleLoggerFactory = new ConsoleLoggerFactory(true) { MinLevel = LogLevel.Debug };
                var queue = $"{typeof(Program).Namespace}.queue";
                services.AddSingleton(consoleLoggerFactory);
                services.AutoRegisterHandlersFromAssemblyOf<SimpleEventHandler>();
                services.AddRebus((configurer, serviceProvider) => configurer
                    .Logging(l => l.Use(consoleLoggerFactory))
                    .Transport(t => t.UseKafka(kafkaEndpoint, queue, "temp"))
                    .UseSchemaRegistryJson(new SchemaRegistryConfig { Url = confluentSchemaRegistry }, true /* set false for production! */)
                    , isDefaultBus: true, key: "Bus");
                services.AddRebus((configurer, serviceProvider) => configurer
                    .Logging(l => l.Use(consoleLoggerFactory))
                    .Transport(t => t.UseKafkaAsOneWayClient(kafkaEndpoint))
                    .UseSchemaRegistryJson(new SchemaRegistryConfig { Url = confluentSchemaRegistry }, true /* set false for production! */)
                    .Options(o => o.InsertStepAfterAutoHeadersOutgoingStep(new AddKafkaKeyHeaderByMessageTypeAndIdStep()))
                    .Routing(r => r.TypeBased().Map<TestMessage>(queue))
                    , isDefaultBus: false, key: "OneWayBus");
            });
            return builder;
        }

        internal const int MessageCount = 10;
        internal static Stopwatch sw = Stopwatch.StartNew();
        static readonly string kafkaEndpoint = "confluent-kafka:9092";
        const string confluentSchemaRegistry = "http://confluent-kafka:8081";
    }
}
