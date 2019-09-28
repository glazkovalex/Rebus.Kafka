using System.Threading.Tasks;
using Rebus.Handlers;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Messages
{
    /// <inheritdoc />
    public class MessageHandler : IHandleMessages<Message>
    {
        /// <inheritdoc />
        public Task Handle(Message evnt)
        {
            SimpleTests.Amount += evnt.MessageNumber;
            _output.WriteLine($"Received : \"{evnt.MessageNumber}\"");
            return Task.CompletedTask;
        }

        private readonly ITestOutputHelper _output;

        /// <summary>Creates new instance <see cref="MessageHandler"/>.</summary>
        public MessageHandler(ITestOutputHelper output)
        {
            _output = output;
        }
    }
}
