using Rebus.Activation;
using Rebus.Config;
using Rebus.Kafka;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Transport;
using Scaleout.Messages;
using System.Diagnostics;

namespace RetryStrategy
{
    class Program
    {
        static void Main(string[] args)
        {
            using (BuiltinHandlerActivator adapter = new BuiltinHandlerActivator(), oneWayAdapter = new BuiltinHandlerActivator())
            {
                var amount = 0;
                Stopwatch sw = Stopwatch.StartNew();
                adapter.Handle<TestMessage>(message =>
                {
                    if (message.MessageNumber == 2)
                    {
                        counter++;
                        throw new InvalidOperationException($"Test ErrorHandling. Counter = {counter}");
                    }
                    Interlocked.Add(ref amount, message.MessageNumber);
                    WriteBlueLine($"Received : \"{message.MessageNumber}\"");
                    if (message.MessageNumber == MessageCount)
                        WriteBlueLine($"Received {MessageCount} messages for {sw.ElapsedMilliseconds / 1000f:N3}s");
                    return Task.CompletedTask;
                });

                Configure.With(adapter)
                    .Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Debug))
                    .Transport(t => t.UseKafka(kafkaEndpoint, $"{typeof(Program).Namespace}.queue", "temp"))
                    .Options(b => b.RetryStrategy(maxDeliveryAttempts: 3, errorQueueName: "retries.on.error.queue"))
                    .Start();

                Configure.With(oneWayAdapter)
                    .Logging(l => l.ColoredConsole(Rebus.Logging.LogLevel.Debug))
                    .Transport(t => t.UseKafkaAsOneWayClient(kafkaEndpoint))
                    .Routing(r => r.TypeBased().Map<TestMessage>($"{typeof(Program).Namespace}.queue"))
                    .Start();

                char key;
                do
                {
                    using (var scope = new RebusTransactionScope()) // Only for example
                    {
                        var sendAmount = 0;
                        var messages = Enumerable.Range(1, MessageCount)
                            .Select(i =>
                            {
                                sendAmount = sendAmount + i;
                                return oneWayAdapter.Bus.Send(new TestMessage { MessageNumber = i });
                            }).ToArray();

                        Task.WaitAll(messages);
                        WriteBlueLine("Sent, but without scope.Complete");
                        scope.Complete(); // Only for example
                        WriteBlueLine($"Send: {sendAmount} for {sw.ElapsedMilliseconds / 1000f:N3}s");
                    }
                    WriteBlueLine("Press any key to exit or 'r' to repeat.");
                    key = Console.ReadKey().KeyChar;
                } while (key == 'r' || key == 'к');
            }

            Console.ReadKey();
        }

        private static void WriteBlueLine(string text)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(text);
            Console.ForegroundColor = ConsoleColor.Gray;
        }

        static int counter = 0;
        const int MessageCount = 50;
        static readonly string kafkaEndpoint = "confluent-kafka:9092";
    }
}
