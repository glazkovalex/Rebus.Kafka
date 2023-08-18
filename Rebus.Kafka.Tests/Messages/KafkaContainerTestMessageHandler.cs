using System.Threading.Tasks;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Messages
{
    /// <inheritdoc />
    public class KafkaContainerTestMessageHandler : IHandleMessages<Message>
    {
        /// <inheritdoc />
        public Task Handle(Message evnt)
        {
            Counter.Add(evnt.MessageNumber);
            _output.WriteLine($"MessageHandler received : \"{evnt.MessageNumber}\"");
            return Task.CompletedTask;
        }

        internal static Counter Counter = new Counter();
        private readonly ITestOutputHelper _output;

        /// <summary>Creates new instance <see cref="MessageHandler"/>.</summary>
        public KafkaContainerTestMessageHandler(ITestOutputHelper output)
        {
            _output = output;
        }
    }
}
