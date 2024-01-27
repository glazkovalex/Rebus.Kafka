using Rebus.Activation;
using Rebus.Config;
using Rebus.Kafka.NTests.Messages;
using Rebus.Kafka.NTests.Transports;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using System.Diagnostics;

namespace Rebus.Kafka.NTests
{
    [TestFixture]
    public class TestingWithContainers : SetUpTestWithKafkaContainer
    {
        [SetUp]
        public void Setup()
        {
            Console.WriteLine("Setuped");
        }

        [Test]
        public async Task Can_Call_Endpoint()
        {
            const int perfomanceCount = 10000;
            using (var adapter = new BuiltinHandlerActivator())
            {
                Stopwatch swHandle = null;
                int messageCount = 0;
                var sendAmount = 0;
                adapter.Handle<TestMessage>(message =>
                {
                    swHandle ??= Stopwatch.StartNew();
                    sendAmount -= message.MessageNumber;
                    messageCount++;
                    if (messageCount == perfomanceCount && sendAmount == 0)
                    {
                        swHandle.Stop();
                        Console.WriteLine($"Rebus received {perfomanceCount} messages in {swHandle.ElapsedMilliseconds / 1000f:N3}s");
                    }
                    return Task.CompletedTask;
                });

                Configure.With(adapter)
                    .Logging(l => l.Console(LogLevel.Info))
                    .Transport(t => t.UseKafka(BootstrapServer, $"{nameof(TestingWithContainers)}.queue"))
                    .Routing(r => r.TypeBased().Map<TestMessage>($"{nameof(TestingWithContainers)}.queue"))
                    .Start();

                var messages = Enumerable.Range(1, perfomanceCount);
                sendAmount = messages.Sum();
                Stopwatch swSend = Stopwatch.StartNew();
                var jobs = messages.Select(i => adapter.Bus.Send(new TestMessage { MessageNumber = i }));
                await Task.WhenAll(jobs);

                swSend.Stop();
                Console.WriteLine($"Rebus send {perfomanceCount} messages in {swSend.ElapsedMilliseconds / 1000f:N3}s.");
                Assert.That(swSend.ElapsedMilliseconds < 2000);

                await Task.Delay(20000);
                Assert.That(swHandle.IsRunning == false && swHandle?.ElapsedMilliseconds < 20000);
            }
        }
    }
}
