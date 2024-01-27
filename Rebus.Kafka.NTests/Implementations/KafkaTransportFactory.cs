using Rebus.Logging;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading;
using Rebus.Threading.SystemThreadingTimer;
using Rebus.Transport;
using System.Collections.Concurrent;

namespace Rebus.Kafka.NTests.Implementations
{
    public class KafkaTransportFactory : ITransportFactory
    {
        public void CleanUp()
        {
            foreach (var transport in Transports)
            {
                transport.Dispose();
            }
        }

        public ITransport Create(string inputQueueAddress)
        {
            //var adapter = new BuiltinHandlerActivator();
            //Adapters.Add(adapter);
            //    adapter.Handle<Message>((bus, messageContext, message) =>
            //{
            //    amount = amount + message.MessageNumber;
            //    string headers = string.Join(", ", messageContext.Headers.Select(h => $"{h.Key}:{h.Value}"));
            //    Logger.LogTrace($"Received : \"{message.MessageNumber}\" with heders: {headers}");
            //    if (message.MessageNumber == MessageCount)
            //        Logger.LogTrace($"Received {MessageCount} messages in {sw.ElapsedMilliseconds / 1000f:N3}s");
            //    return Task.CompletedTask;
            //});
            //Logger.LogTrace($"Current kafka endpoint: {BootstrapServer}");

            //Configure.With(adapter)
            //    .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
            //    .Transport(t => t.UseKafka(BootstrapServer, nameof(SimpleTests), "sample-consumer"))
            //    //.Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
            //    .Start();
            IRebusLoggerFactory loggerFactory = new ConsoleLoggerFactory(true) { MinLevel = LogLevel.Debug };
            IAsyncTaskFactory asyncTaskFactory = new SystemThreadingTimerAsyncTaskFactory(loggerFactory);
            KafkaTransport transport = new KafkaTransport(loggerFactory, asyncTaskFactory, _bootstrapserver, inputQueueAddress);
            transport.Initialize();
            Transports.Add(transport);
            return transport;
        }

        public ITransport CreateOneWayClient()
        {
            IRebusLoggerFactory loggerFactory = new ConsoleLoggerFactory(true) { MinLevel = LogLevel.Debug };
            IAsyncTaskFactory asyncTaskFactory = new SystemThreadingTimerAsyncTaskFactory(loggerFactory);
            KafkaTransport transport = new KafkaTransport(loggerFactory, asyncTaskFactory, _bootstrapserver, null);
            Transports.Add(transport);
            transport.Initialize();
            return transport;
        }

        protected ConcurrentBag<KafkaTransport> Transports = new ConcurrentBag<KafkaTransport>();
        static string _bootstrapserver;
        internal static void initialization(string bootstrapserver)
        {
            _bootstrapserver = bootstrapserver;
        }
    }
}
