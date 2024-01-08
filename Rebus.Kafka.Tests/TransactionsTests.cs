using Autofac;
using Microsoft.Extensions.Logging;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Rebus.Kafka.Tests.Messages;
using Rebus.Transport;
using System.Linq;
using System.Threading.Tasks;
using Testcontainers.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public class TransactionsTests : BaseTestWithKafkaContainer
    {
        [Fact]
        public async Task IncompleteTransaction()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );
            MessageHandler.Counter.Reset();
            using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                await bus.Subscribe<Message>();

                using (var scope = new RebusTransactionScope())
                {
                    var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        return bus.Publish(new Message { MessageNumber = i });
                    }).ToArray();
                    Task.WaitAll(messages);
                    await Task.Delay(10000);

                    Output.WriteLine("Sent, but without scope.Complete");
                    Assert.Equal(0, MessageHandler.Counter.Amount);
                }
                Output.WriteLine("Сompleting a transaction without scope.Complete\"");
                await Task.Delay(10000);
                Assert.Equal(0, MessageHandler.Counter.Amount);

                var sendAmount = 0;
                using (var scope = new RebusTransactionScope())
                {
                    var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        sendAmount = sendAmount + i;
                        return bus.Publish(new Message { MessageNumber = i });
                    }).ToArray();
                    Task.WaitAll(messages);
                    await Task.Delay(10000);

                    Output.WriteLine("Sent, but without scope.Complete");
                    Assert.Equal(0, MessageHandler.Counter.Amount);

                    await scope.CompleteAsync();
                    Output.WriteLine("Now scope.Complete");
                }
                await Task.Delay(10000);
                Assert.Equal(sendAmount, MessageHandler.Counter.Amount);
            }
        }        

        [Fact]
        public async Task DelayedCompletionOfTransaction()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );
            MessageHandler.Counter.Reset();
            using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                await bus.Subscribe<Message>();
                var sendAmount = 0;
                using (var scope = new RebusTransactionScope())
                {
                    var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        sendAmount = sendAmount + i;
                        return bus.Publish(new Message { MessageNumber = i });
                    }).ToArray();
                    Task.WaitAll(messages);
                    await Task.Delay(10000);

                    Output.WriteLine("Sent, but without scope.Complete");
                    Assert.Equal(0, MessageHandler.Counter.Amount);

                    await scope.CompleteAsync();
                    Output.WriteLine("Now scope.Complete");
                }
                await Task.Delay(10000);
                Assert.Equal(sendAmount, MessageHandler.Counter.Amount);
            }
        }

        const int MessageCount = 10;
        public TransactionsTests(ITestOutputHelper output) : base(output) { }
    }
}
