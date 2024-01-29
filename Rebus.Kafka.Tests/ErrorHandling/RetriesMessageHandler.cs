using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Messages;
using Rebus.Retry.Simple;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.ErrorHandling
{
    /// <inheritdoc />
    public class RetriesMessageHandler : IHandleMessages<RetriesMessage>, IHandleMessages<IFailed<RetriesMessage>>
    {
        /// <inheritdoc />
        public async Task Handle(RetriesMessage evnt)
        {
            ErrorHandlingTests.Counter.Add(evnt);
            _output.WriteLine($"RetriesMessageHandler received : \"{evnt.MessageNumber}\". Amount = {ErrorHandlingTests.Counter.Amount}");
            if (evnt.MessageNumber == 2)
            {
                throw new InvalidOperationException("Checking for a message processing failure");
            }
            else
            {
                await Task.Delay(100);
            }
        }

        public async Task Handle(IFailed<RetriesMessage> failedMessage)
        {
            var deferCount = Convert.ToInt32(failedMessage.Headers.GetValueOrDefault(Rebus.Messages.Headers.DeferCount));
            _output.WriteLine($"RetriesMessageHandler deferCount:{deferCount} processing IFailed<RetriesMessage> : \"{failedMessage.Message.MessageNumber}\"");
            _output.WriteLine($"ErrorDescription: {failedMessage.ErrorDescription}");
            _output.WriteLine($"Exceptions: {JsonSerializer.Serialize(failedMessage.Exceptions, new JsonSerializerOptions { WriteIndented = true })}");
            ErrorHandlingTests.Counter.Add(failedMessage.Message);
            
            const int maxDeferCount = 2;
            if (deferCount >= maxDeferCount)
            {
                await _bus.Advanced.TransportMessage.Deadletter($"Failed after {deferCount} deferrals\n\n{failedMessage.ErrorDescription}");
            }
            else
            {
                await _bus.Advanced.TransportMessage.Defer(TimeSpan.FromSeconds(1));
            }
        }

        readonly ITestOutputHelper _output;
        IBus _bus;

        /// <summary>Creates new instance <see cref="RetriesMessageHandler"/>.</summary>
        public RetriesMessageHandler(ITestOutputHelper output, IBus bus)
        {
            _output = output;
            _bus = bus;
        }
    }
}
