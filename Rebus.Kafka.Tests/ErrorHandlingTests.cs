using Autofac;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Kafka.Tests.Messages;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public class ErrorHandlingTests : BaseTestWithServicesFixture
    {
        [Fact(Skip = "Doesn't work at the moment")]
        public async Task HandlingErrorInSubscribe()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<RetriesMessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(RetriesMessage)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(Fixture.KafkaEndpoint, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
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

                Assert.Equal(Counter.Amount, sendAmount*3);
            }
        }

        [Fact]
        public async Task RetriesOnError()
        {
            
        }

        #region Settings

        internal static Counter Counter;
        const int MessageCount = 1;

        /// <summary>Creates new instance <see cref="SimpleTests"/>.</summary>
        public ErrorHandlingTests(ServicesFixture fixture, ITestOutputHelper output) : base(fixture, output)
        {
            Counter = new Counter();
        }

        #endregion
    }
}
