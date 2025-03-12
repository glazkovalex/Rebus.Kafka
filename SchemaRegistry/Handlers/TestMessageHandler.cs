using Rebus.Handlers;
using SchemaRegistry.Messages;

namespace SchemaRegistry.Handlers
{
    internal class TestMessageHandler : IHandleMessages<TestMessage>
    {
        public Task Handle(TestMessage message)
        {
            Interlocked.Add(ref amount, message.MessageNumber);
            Console.WriteLine($"Received : \"{message.MessageNumber}\" (Thread #{Thread.CurrentThread.ManagedThreadId})");
            if (message.MessageNumber == Program.MessageCount)
                Console.WriteLine($"Received {Program.MessageCount} messages for {Program.sw.ElapsedMilliseconds / 1000f:N3}s");
            return Task.CompletedTask;
        }

        static int amount = 0;
    }
}
