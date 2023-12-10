using System.Threading.Tasks;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Messages
{
    /// <inheritdoc />
    public class SecondMessageHandler : IHandleMessages<SecondMessage>
    {
        /// <inheritdoc />
        public Task Handle(SecondMessage evnt)
        {
            Counter.Add(evnt.MessageNumber);
            _output.WriteLine($"SecondMessageHandler received : \"{evnt.MessageNumber}\"");
            return Task.CompletedTask;
        }

        internal static Counter Counter = new Counter();
        private readonly ITestOutputHelper _output;

        /// <summary>Creates new instance <see cref="SecondMessageHandler"/>.</summary>
        public SecondMessageHandler(ITestOutputHelper output)
        {
            _output = output;
        }
    }
}
