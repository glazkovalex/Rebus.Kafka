using Confluent.Kafka;
using IdempotentSaga.Handlers;
using IdempotentSaga.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Kafka;
using Rebus.Kafka.Configs;
using Rebus.Logging;
using Rebus.PostgreSql;
using Rebus.Retry.Simple;
using Rebus.Sagas.Idempotent;

namespace IdempotentSaga
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            using (var host = CreateHostBuilder(args).Build())
            {
                IBus bus = host.Services.GetRequiredService<IBus>();
                AllSubscriptions(bus).Wait(); //Do not use await and "OnCreated" to avoid deadlocking!
                char key;
                do
                {
                    var message = new KickoffSagaMessages { SagaInstanceId = Guid.NewGuid() };
                    await bus.Publish(message);
                    WriteBlueLine("Press any key to exit or 'r' to repeat.");
                    key = Console.ReadKey().KeyChar; 
                } while (key == 'r' || key == 'к');
                await host.StopAsync();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args);
            builder.ConfigureServices((hostContext, services) =>
            {
                var consoleLoggerFactory = new ConsoleLoggerFactory(true) { MinLevel = LogLevel.Debug };
                //consoleLoggerFactory.Filters.Add(ls => ls.Text != "Checking outbox storage for pending messages" && ls.Text != "No pending messages found");
                services.AddSingleton(consoleLoggerFactory);
                services.AutoRegisterHandlersFromAssemblyOf<TestSaga>();
                var consumerConfig = new ConsumerAndBehaviorConfig(kafkaEndpoint, "temp") { BehaviorConfig = new ConsumerBehaviorConfig { CommitPeriod = 5 } };
                services.AddRebus((configurer, serviceProvider) => configurer
                    .Logging(l => l.Use(consoleLoggerFactory))
                    .Transport(t => t.UseKafka(kafkaEndpoint, $"{nameof(IdempotentSaga)}.queue", new ProducerConfig(), consumerConfig))
                    //.Outbox(o => o.StoreInPostgreSql(connectionString, "rebus.Outbox"))
                    //.RetryStrategy(maxDeliveryAttempts: 3, errorQueueName: $"{nameof(IdempotentSaga)}.queue.error"))
                    .Sagas(s => s.StoreInPostgres(connectionString, "SagasData", "SagasIndex", true, null, schemaName: "rebus"))
                    .Timeouts(t => t.StoreInPostgres(new PostgresConnectionHelper(connectionString), "Timeouts", true, "rebus"))
                    .Options(b =>
                    {
                        b.EnableIdempotentSagas();
                        b.UseAttributeOrTypeFullNameForTopicNames();
                    })
                );
            });
            return builder;
        }

        internal static async Task AllSubscriptions(IBus bus)
        {
            await bus.Subscribe<KickoffSagaMessages>();
            await bus.Subscribe<SagaMessageEarth>();
            await bus.Subscribe<SagaMessageWind>();
            await bus.Subscribe<SagaMessageFire>();
        }

        internal static void WriteBlueLine(string text)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(text);
            Console.ForegroundColor = ConsoleColor.Gray;
        }

        static int counter = 0;
        internal const int MessageCount = 1;
        static readonly string kafkaEndpoint = "confluent-kafka:9092";
        static readonly string connectionString = "Host=Ubuntu-PostgreSQL;Database=Rebus;Username=demo;Password=1;";
    }
}
