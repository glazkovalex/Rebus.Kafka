using System;
using System.Threading.Tasks;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Messages;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.ErrorHandling
{
    /// <inheritdoc />
    public class RetriesMessageHandler : IHandleMessages<RetriesMessage>
    {
        /// <inheritdoc />
        public Task Handle(RetriesMessage evnt)
        {
            ErrorHandlingTests.Counter.Add(evnt.MessageNumber);
            _output.WriteLine($"RetriesMessageHandler received : \"{evnt.MessageNumber}\"");
            if (ErrorHandlingTests.Counter.Count < 2)
            {
                throw new InvalidOperationException();
            }
            return Task.CompletedTask;
        }

        private readonly ITestOutputHelper _output;

        /// <summary>Creates new instance <see cref="RetriesMessageHandler"/>.</summary>
        public RetriesMessageHandler(ITestOutputHelper output)
        {
            _output = output;
        }
    }
}
