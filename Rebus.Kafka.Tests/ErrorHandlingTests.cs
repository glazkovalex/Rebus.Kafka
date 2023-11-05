using System;
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
using Rebus.Logging;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Transport.InMem;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public class ErrorHandlingTests : BaseTestWithKafkaContainer
    {
        [Fact/*(Skip = "Doesn't work at the moment")*/]
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
                    o.SimpleRetryStrategy();
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
                        sendAmount = sendAmount + i;
                        return bus.Publish(new RetriesMessage { MessageNumber = i });
                    }).ToArray();

                Task.WaitAll(messages);
                await Task.Delay(10000);

                Assert.Equal(Counter.Amount, sendAmount * 3);
            }
        }

        [Fact]
        public async Task RetriesOnError()
        {
            const int numberOfRetries = 5;
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

                    o.SimpleRetryStrategy(maxDeliveryAttempts: numberOfRetries, errorQueueAddress: "retries.on.error.queue");
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
                    //throw new InvalidOperationException("omgwtf!");
                });
            }
            Action();
            api.SetNumberOfWorkers(count);

            await bus.Send("hej");

            await Task.Delay(10000);

            Assert.Equal(numberOfRetries, attemptedDeliveries);
        }

        #region Settings

        internal static Counter Counter;
        const int MessageCount = 1;

        /// <summary>Creates new instance <see cref="SimpleTests"/>.</summary>
        public ErrorHandlingTests(ITestOutputHelper output) : base(output)
        {
            Counter = new Counter();
        }

        #endregion
    }
}
