using Autofac;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Rebus.Routing.TypeBased;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Kafka.Tests.Messages;
using Rebus.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    //[Collection("ServicesFixture")]
    public class SimpleTests : BaseTestWithServicesFixture
    {
        [Fact]
        public async Task SendReceive()
        {
            using (var adapter = new BuiltinHandlerActivator())
            {
                var amount = 0;
                Stopwatch sw = Stopwatch.StartNew();

                adapter.Handle<Message>(message =>
                {
                    amount = amount + message.MessageNumber;
                    Output.WriteLine($"Received : \"{message.MessageNumber}\"");
                    if (message.MessageNumber == MessageCount)
                        Output.WriteLine($"Received {MessageCount} messages in {sw.ElapsedMilliseconds / 1000f:N3}s");
                    return Task.CompletedTask;
                });

                Configure.With(adapter)
                    .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                    .Transport(t => t.UseKafka(Fixture.KafkaEndpoint, nameof(SimpleTests), "temp"))
                    .Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
                    .Start();

                var sendAmount = 0;
                var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        sendAmount = sendAmount + i;
                        return adapter.Bus.Send(new Message { MessageNumber = i });
                    }).ToArray();

                Task.WaitAll(messages);
                await Task.Delay(10000);

                Assert.Equal(amount, sendAmount);
            }
        }

        [Fact]
        public async Task PublishSubscribe()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(Fixture.KafkaEndpoint, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );

            using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                await bus.Subscribe<Message>();
                var sendAmount = 0;
                var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                   {
                       sendAmount = sendAmount + i;
                       return bus.Publish(new Message { MessageNumber = i });
                   }).ToArray();

                Task.WaitAll(messages);
                await Task.Delay(10000);

                Assert.Equal(Amount, sendAmount);
            }
        }

        [Fact]
        public async Task Performance()
        {
            int perfomanceCount = 10000;
            using (var adapter = new BuiltinHandlerActivator())
            {
                Stopwatch swHandle = null;

                adapter.Handle<Message>(message =>
                {
                    if (swHandle == null)
                    {
                        swHandle = Stopwatch.StartNew();
                    }
                    if (message.MessageNumber == perfomanceCount)
                    {
                        swHandle.Stop();
                        Output.WriteLine($"Received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                    }
                    return Task.CompletedTask;
                });

                Configure.With(adapter)
                    .Logging(l => l.Use(new TestOutputLoggerFactory(Output) { MinLevel = LogLevel.Warn }))
                    .Transport(t => t.UseKafka(Fixture.KafkaEndpoint, nameof(SimpleTests), "temp"))
                    .Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
                    .Start();

                var messages = Enumerable.Range(1, perfomanceCount)
                    .Select(i => adapter.Bus.Send(new Message { MessageNumber = i })).ToArray();
                Stopwatch swSend = Stopwatch.StartNew();

                Task.WaitAll(messages);

                swSend.Stop();
                Output.WriteLine($"Send {perfomanceCount} messages in {swSend.ElapsedMilliseconds / 1000f:N3}s.");

                await Task.Delay(10000);
                Assert.True(swSend.ElapsedMilliseconds < 10000);
            }
        }

        #region Settings

        internal static int Amount;
        const int MessageCount = 10;

        /// <summary>Creates new instance <see cref="SimpleTests"/>.</summary>
        public SimpleTests(ServicesFixture fixture, ITestOutputHelper output) : base(fixture, output) { }

        #endregion
    }
}
