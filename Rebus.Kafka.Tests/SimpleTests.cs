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
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using Confluent.Kafka;

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
        public async Task RebusPerformance()
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
                                    Output.WriteLine($"Rebus received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                                }
                                return Task.CompletedTask;
                            });

                Configure.With(adapter)
                    .Logging(l => l.Use(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Warn }))
                    .Transport(t => t.UseKafka(Fixture.KafkaEndpoint, nameof(SimpleTests), "temp"))
                    .Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
                    .Start();

                Stopwatch swSend = Stopwatch.StartNew();
                var messages = Enumerable.Range(1, perfomanceCount)
                    .Select(i => adapter.Bus.Send(new Message { MessageNumber = i }));
                await Task.WhenAll(messages);

                swSend.Stop();
                Output.WriteLine($"Rebus send {perfomanceCount} messages in {swSend.ElapsedMilliseconds / 1000f:N3}s.");

                Assert.True(swSend.ElapsedMilliseconds < 10000);
                await Task.Delay(10000);
                Assert.True(swHandle?.ElapsedMilliseconds < 10000);
            }
        }

        [Fact(Skip = "Doesn't work at the moment")]
        public async Task ConfluentPerformance()
        {
            int perfomanceCount = 10000;
            
            CancellationTokenSource cts = new CancellationTokenSource();
            using (var producer = new KafkaProducer(Fixture.KafkaEndpoint))
            using (KafkaConsumer consumer = new KafkaConsumer(Fixture.KafkaEndpoint, (ILogger<KafkaConsumer>) null))
            {
                Stopwatch swHandle = null;

                consumer.Consume(new[] {"temp"})
                    .Subscribe(message =>
                    {
                        if (swHandle == null)
                        {
                            swHandle = Stopwatch.StartNew();
                        }

                        if (int.Parse(message.Value) == perfomanceCount)
                        {
                            swHandle.Stop();
                            Output.WriteLine(
                                $"Confluent received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                            cts.Cancel();
                        }
                    } , cts.Token);

                Stopwatch sw = Stopwatch.StartNew();
                var jobs = Enumerable.Range(1, perfomanceCount)
                    .Select(i => new Message<Null, string> {Value = i.ToString()})
                    .Select(m => producer.ProduceAsync("temp", m)).ToArray();
                await Task.WhenAll(jobs);
                Output.WriteLine($"Confluent send {perfomanceCount} in {sw.ElapsedMilliseconds / 1000:N3}с");

                Assert.True(sw.ElapsedMilliseconds < 10000);

                await Task.Delay(10000);
                //cts.Cancel();
                await Task.Delay(100);
                Assert.True(swHandle?.ElapsedMilliseconds < 10000);
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
