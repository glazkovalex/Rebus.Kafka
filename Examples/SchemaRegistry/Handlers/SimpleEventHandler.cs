using Rebus.Handlers;
using SchemaRegistry.Events;

namespace SchemaRegistry.Handlers
{
    internal class SimpleEventHandler : IHandleMessages<SimpleEvent>
    {
        public Task Handle(SimpleEvent evnt)
        {
            Interlocked.Add(ref amount, evnt.EventNumber);
            Console.WriteLine($"Received : \"{evnt.EventNumber}\" (Thread #{Thread.CurrentThread.ManagedThreadId})");
            if (evnt.EventNumber == Program.MessageCount)
                Console.WriteLine($"Received {Program.MessageCount} events for {Program.sw.ElapsedMilliseconds / 1000f:N3}s");
            return Task.CompletedTask;
        }

        static int amount = 0;
    }
}