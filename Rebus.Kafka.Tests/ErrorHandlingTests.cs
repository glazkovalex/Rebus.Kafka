using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Rebus.Kafka.Tests.ErrorHandling;
using Rebus.Kafka.Tests.Messages;
using Rebus.Persistence.InMem;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public class ErrorHandlingTests : BaseTestWithKafkaContainer
    {
        [Fact]
        public async Task HandlingErrorInSubscribe()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<RetriesMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(RetriesMessage)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, nameof(ErrorHandlingTests)))
                .Options(o =>
                {
                    o.RetryStrategy(maxDeliveryAttempts: maxDeliveryAttempts);
                    o.SetMaxParallelism(5);
                })
            );

            await using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                await bus.Subscribe<RetriesMessage>();
                var sendAmount = 0;
                var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        Interlocked.Add(ref sendAmount, i);
                        return bus.Publish(new RetriesMessage { MessageNumber = i });
                    }).ToArray();

                Task.WaitAll(messages);
                await Task.Delay(10000);

                Assert.Equal(sendAmount + (maxDeliveryAttempts - 1) * 2, Counter.Amount);
            }
        }

        [Fact]
        public async Task RetriesOnError()
        {
            const string inputQueueName = "test.error.handling.tests";
            var handlerActivator = new BuiltinHandlerActivator();

            var bus = Configure.With(handlerActivator)
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, inputQueueName))
                .Routing(r => r.TypeBased().Map<string>(inputQueueName))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                    o.RetryStrategy(maxDeliveryAttempts: maxDeliveryAttempts, errorQueueName: "retries.on.error.queue");
                })
                .Start();

            var attemptedDeliveries = 0;

            var api = bus.Advanced.Workers;
            var count = api.Count;
            api.SetNumberOfWorkers(0);
            void Action()
            {
                handlerActivator.Handle<string>(async _ =>
                {
                    Interlocked.Increment(ref attemptedDeliveries);
                    throw new System.InvalidOperationException("omgwtf!");
                });
            }
            Action();
            api.SetNumberOfWorkers(count);

            await bus.Send("hej");

            await Task.Delay(10000);

            Assert.Equal(maxDeliveryAttempts, attemptedDeliveries);
        }

        [Fact]
        public async Task SecondLevelRetries()
        {
            var maxDelivery = 2;
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<RetriesMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(RetriesMessage)));
            builder.RegisterType<RetriesMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(IFailed<RetriesMessage>)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, nameof(ErrorHandlingTests)))
                .Timeouts(t => t.StoreInMemory())
                .Options(o =>
                {
                    o.RetryStrategy(maxDeliveryAttempts: maxDelivery, secondLevelRetriesEnabled: true);
                    o.SetMaxParallelism(1);
                })
            );

            await using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                await bus.Subscribe<RetriesMessage>();
                var sendAmount = 0;
                var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        Interlocked.Add(ref sendAmount, i);
                        return bus.Publish(new RetriesMessage { MessageNumber = i });
                    }).ToArray();
                Task.WaitAll(messages);
                await Task.Delay(10000);
                messages = Enumerable.Range(MessageCount + 1, MessageCount)
                    .Select(i =>
                    {
                        Interlocked.Add(ref sendAmount, i);
                        return bus.Publish(new RetriesMessage { MessageNumber = i });
                    }).ToArray();
                Task.WaitAll(messages);
                await Task.Delay(10000);
                messages = Enumerable.Range(MessageCount * 2 +1, MessageCount)
                    .Select(i =>
                    {
                        Interlocked.Add(ref sendAmount, i);
                        return bus.Publish(new RetriesMessage { MessageNumber = i });
                    }).ToArray();
                Task.WaitAll(messages);
                await Task.Delay(10000);
                Assert.Equal(sendAmount + ((maxDelivery + 1 /*Handle IFailed*/) * 3 /*restarts*/ - 1) * 2, Counter.Amount);
            }
        }

        #region Settings

        internal static Counter Counter;
        const int MessageCount = 10;
        const int maxDeliveryAttempts = 3;

        /// <summary>Creates new instance <see cref="SimpleTests"/>.</summary>
        public ErrorHandlingTests(ITestOutputHelper output) : base(output)
        {
            Counter = new Counter();
        }

        #endregion
    }
}
