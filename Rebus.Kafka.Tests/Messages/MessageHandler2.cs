using System.Threading.Tasks;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Messages
{
    /// <inheritdoc />
    public class MessageHandler2 : IHandleMessages<Message>
    {
        /// <inheritdoc />
        public Task Handle(Message evnt)
        {
            Counter.Add(evnt);
            _output.WriteLine($"MessageHandler2 received : \"{evnt.MessageNumber}\"");
            return Task.CompletedTask;
        }

        internal static Counter Counter = new Counter();
        private readonly ITestOutputHelper _output;

        /// <summary>Creates new instance <see cref="MessageHandler"/>.</summary>
        public MessageHandler2(ITestOutputHelper output)
        {
            _output = output;
        }
    }
}
