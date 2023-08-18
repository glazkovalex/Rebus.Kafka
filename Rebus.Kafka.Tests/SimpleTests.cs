using Autofac;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Configs;
using Rebus.Kafka.Tests.Core;
using Rebus.Kafka.Tests.Messages;
using Rebus.Routing.TypeBased;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Testcontainers.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public sealed class SimpleTests : BaseTestWithKafkaContainer
    {
        [Fact]
        public async Task SendReceiveWithHeader()
        {
            using (var adapter = new BuiltinHandlerActivator())
            {
                var amount = 0;
                Stopwatch sw = Stopwatch.StartNew();

                adapter.Handle<Message>((bus, messageContext, message) =>
                {
                    amount = amount + message.MessageNumber;
                    string headers = string.Join(", ", messageContext.Headers.Select(h => $"{h.Key}:{h.Value}"));
                    Logger.LogTrace($"Received : \"{message.MessageNumber}\" with heders: {headers}");
                    if (message.MessageNumber == MessageCount)
                        Logger.LogTrace($"Received {MessageCount} messages in {sw.ElapsedMilliseconds / 1000f:N3}s");
                    return Task.CompletedTask;
                });
                Logger.LogTrace($"Current kafka endpoint: {BootstrapServer}");

                Configure.With(adapter)
                    .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                    .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests), "sample-consumer"))
                    .Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
                    .Start();

                var sendAmount = 0;
                try
                {
                    var messages = Enumerable.Range(1, MessageCount)
                    .Select(i =>
                    {
                        sendAmount = sendAmount + i;
                        string headerKeyValue = Guid.NewGuid().ToString();
                        return adapter.Bus.Send(new Message { MessageNumber = i }, new Dictionary<string, string> { { "kafka-key", headerKeyValue } });
                    }).ToArray();

                    Task.WaitAll(messages);
                    await Task.Delay(10000);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, e.Message);
                    throw;
                }
                Assert.Equal(amount, sendAmount);
            }
        }

        [Fact]
        public async Task SendReceiveWithConfigs()
        {
            IContainer container;
            var builder = new ContainerBuilder();
            builder.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServer };
            var consumerConfig = new ConsumerAndBehaviorConfig
            {
                BehaviorConfig = new ConsumerBehaviorConfig { CommitPeriod = 10 },
                BootstrapServers = BootstrapServer,
                AllowAutoCreateTopics = true,
                GroupId = nameof(SimpleTests),
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            string kafkaEndpoint = BootstrapServer;
            builder.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), producerConfig, consumerConfig))
                .Routing(r => r.TypeBased().MapAssemblyOf<Message>(nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );
            MessageHandler.Counter.Reset();
            using (container = builder.Build())
            using (IBus bus = container.Resolve<IBus>())
            {
                var answerToTheUltimateQuestionOfLifeTheUniverseAndEverything = 42;
                await bus.Send(new Message { MessageNumber = answerToTheUltimateQuestionOfLifeTheUniverseAndEverything });
                await Task.Delay(10000);
                Logger.LogTrace($"SendReceiveWithConfigs received: {MessageHandler.Counter.Amount}");
                Assert.Equal(MessageHandler.Counter.Amount, answerToTheUltimateQuestionOfLifeTheUniverseAndEverything);
            }
        }

        [Fact]
        [Trait(nameof(DockerCli.DockerPlatform), nameof(DockerCli.DockerPlatform.Linux))]
        public async Task ConsumerReturnsProducerMessage()
        {
            // Given
            const string topic = "sample";

            var producerConfig = new ProducerConfig();
            producerConfig.BootstrapServers = BootstrapServer;

            var consumerConfig = new ConsumerConfig();
            consumerConfig.BootstrapServers = BootstrapServer;
            consumerConfig.GroupId = "sample-consumer";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;

            var message = new Message<string, string>();
            message.Value = Guid.NewGuid().ToString("D");

            // When
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServer }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                        new TopicSpecification { Name = topic, ReplicationFactor = 1, NumPartitions = 1 }
                    }, new CreateTopicsOptions { ValidateOnly = false });
                    var topicMetadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics.FirstOrDefault(t => t.Topic == topic);
                }
                catch (CreateTopicsException e)
                {
                    Logger.LogError($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                    throw;
                }
            }

            ConsumeResult<string, string> result;
            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);

                using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
                {
                    _ = await producer.ProduceAsync(topic, message)
                        .ConfigureAwait(false);
                }

                result = consumer.Consume(TimeSpan.FromSeconds(15));
                Logger.LogTrace($"ConsumerReturnsProducerMessage received: {result.Message.Value}");
            }

            // Then
            Assert.NotNull(result);
            Assert.Equal(message.Value, result.Message.Value);
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
                .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );
            MessageHandler.Counter.Reset();
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

                Assert.Equal(MessageHandler.Counter.Amount, sendAmount);
            }
        }

        [Fact]
        public async Task TwoPublishSameEvent()
        {
            var builder1 = new ContainerBuilder();
            builder1.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder1.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            builder1.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests)))
                .Options(o => o.SetMaxParallelism(5))
            );
            var builder2 = new ContainerBuilder();
            builder2.RegisterInstance(Output).As<ITestOutputHelper>().SingleInstance();
            builder2.RegisterType<MessageHandler2>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
            builder2.RegisterRebus((configurer, context) => configurer
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, $"{nameof(SimpleTests)}2"))
                .Options(o => o.SetMaxParallelism(5))
            );

            MessageHandler.Counter.Reset();
            using (IContainer container1 = builder1.Build())
            using (IBus bus1 = container1.Resolve<IBus>())
            using (IContainer container2 = builder2.Build())
            using (IBus bus2 = container2.Resolve<IBus>())
            {
                await bus1.Subscribe<Message>();
                await bus2.Subscribe<Message>();

                var sendAmount = 0;
                int i = 1;
                await bus1.Publish(new Message { MessageNumber = i });
                sendAmount += i * 2; // two handlers
                i++;
                await bus2.Publish(new Message { MessageNumber = i });
                sendAmount += i * 2;

                await Task.Delay(10000);

                Assert.Equal(MessageHandler.Counter.Amount + MessageHandler2.Counter.Amount, sendAmount);
            }
        }

        [Fact]
        public async Task RebusPerformance()
        {
            const int perfomanceCount = 10000;
            using (var adapter = new BuiltinHandlerActivator())
            {
                Stopwatch swHandle = null;
                int messageCount = 0;
                var sendAmount = 0;
                adapter.Handle<Message>(message =>
                {
                    swHandle ??= Stopwatch.StartNew();
                    sendAmount -= message.MessageNumber;
                    messageCount++;
                    if (messageCount == perfomanceCount && sendAmount == 0)
                    {
                        swHandle.Stop();
                        Output.WriteLine($"Rebus received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                    }
                    return Task.CompletedTask;
                });

                Configure.With(adapter)
                    .Logging(l => l.Use(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Warn }))
                    .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests), "temp"))
                    .Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
                    .Start();

                var messages = Enumerable.Range(1, perfomanceCount);
                sendAmount = messages.Sum();
                Stopwatch swSend = Stopwatch.StartNew();
                var jobs = messages.Select(i => adapter.Bus.Send(new Message { MessageNumber = i }));
                await Task.WhenAll(jobs);

                swSend.Stop();
                Output.WriteLine($"Rebus send {perfomanceCount} messages in {swSend.ElapsedMilliseconds / 1000f:N3}s.");
                Assert.True(swSend.ElapsedMilliseconds < 2000);

                await Task.Delay(10000);
                Assert.True(swHandle.IsRunning == false && swHandle?.ElapsedMilliseconds < 10000);
            }
        }

        [Fact]
        public async Task ConfluentPerformance()
        {
            string topic = "Performance";
            int perfomanceCount = 10000;
            CancellationTokenSource cts = new CancellationTokenSource();
            var producerLogger = new TestLogger<KafkaProducer>(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Info }.GetLogger<KafkaProducer>());
            using (var producer = new KafkaProducer(BootstrapServer, producerLogger))
            {
                var consumerLogger = new TestLogger<KafkaConsumer>(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Info }.GetLogger<KafkaConsumer>());
                Stopwatch swHandle = null;

                KafkaConsumer consumer = new KafkaConsumer(BootstrapServer, "temp", consumerLogger);
                {
                    int messageCount = 0;
                    var sendAmount = 0;
                    var obs = consumer.Consume(new[] { topic });
                    obs.Subscribe(transportMessage =>
                    {
                        swHandle ??= Stopwatch.StartNew();

                        var message = JsonSerializer.Deserialize<Message>(transportMessage.Value);
                        sendAmount -= message.MessageNumber;
                        messageCount++;
                        if (messageCount == perfomanceCount && sendAmount == 0)
                        {
                            swHandle.Stop();
                            Output.WriteLine($"Confluent received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                            cts.Cancel();
                        }
                    }, cts.Token);
                    await Task.Delay(1000);

                    var messages = Enumerable.Range(1, perfomanceCount);
                    sendAmount = messages.Sum();
                    Stopwatch swSend = Stopwatch.StartNew();
                    var jobs = messages.Select(i => new Message<Null, string> { Value = JsonSerializer.Serialize(new Message { MessageNumber = i }) })
                        .Select(m => producer.ProduceAsync(topic, m));
                    await Task.WhenAll(jobs);
                    swSend.Stop();
                    Output.WriteLine($"Confluent send {perfomanceCount} in {swSend.ElapsedMilliseconds / 1000f:N3}с");
                    Assert.True(swSend.ElapsedMilliseconds < 2000);

                    await Task.Delay(10000);
                    cts.Cancel();
                    Assert.True(swHandle?.IsRunning == false && swHandle?.ElapsedMilliseconds < 10000);
                }
            }
        }

        private readonly KafkaContainer _kafkaContainer = new KafkaBuilder().WithImage("confluentinc/cp-kafka:7.0.1").Build();

        public async Task InitializeAsync()
        {
            Logger.LogTrace("Initialize kafka container");
            await _kafkaContainer.StartAsync();
            BootstrapServer = _kafkaContainer.GetBootstrapAddress();
        }

        public Task DisposeAsync()
        {
            Logger.LogTrace("Dispose kafka container");
            return _kafkaContainer.DisposeAsync().AsTask();
        }

        const int MessageCount = 10;

        public SimpleTests(ITestOutputHelper output) : base(output)
        {
            
        }
    }
}
