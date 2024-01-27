using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;


namespace Rebus.Kafka.Tests.Transports.Implementations
{
    //public class KafkaBusFactory : IBusFactory
    //{
    //    public void Cleanup()
    //    {
    //        foreach (var adapter in Adapters)
    //        {
    //            adapter.Dispose();
    //        }
    //    }

    //    public IBus GetBus<TMessage>(string inputQueueAddress, Func<TMessage, Task> handler)
    //    {
    //        var adapter = new BuiltinHandlerActivator();
    //        Adapters.Add(adapter);
    //        adapter.Handle(handler);
    //        IBus bus = Configure.With(adapter)
    //            .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
    //            .Transport(t => t.UseKafka(BootstrapServer, inputQueueAddress))
    //            .Start();
    //        return bus;
    //    }

    //    protected ConcurrentBag<BuiltinHandlerActivator> Adapters = new ConcurrentBag<BuiltinHandlerActivator>();
    //    protected string BootstrapServer;
    //    protected ITestOutputHelper Output;
    //    internal KafkaBusFactory(string bootstrapServer, ITestOutputHelper output)
    //    {
    //        BootstrapServer = bootstrapServer;
    //        Output = output;
    //    }
    //}
}
