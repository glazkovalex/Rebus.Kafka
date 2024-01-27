using Rebus.Messages;
using Rebus.Transport;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka.Tests.Extensions;

public static class TestTransportEx
{
    public static async Task<TransportMessage> AwaitReceive(this ITransport transport, double timeoutSeconds = 5)
    {
        var stopwatch = Stopwatch.StartNew();
        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var source = new CancellationTokenSource();

        while (stopwatch.Elapsed < timeout)
        {
            TransportMessage receivedTransportMessage;

            using (var scope = new RebusTransactionScope())
            {
                receivedTransportMessage = await transport.Receive(scope.TransactionContext, source.Token);

                await scope.CompleteAsync();
            }
            if (receivedTransportMessage != null) return receivedTransportMessage;
        }

        throw new TimeoutException($"Did not receive transport message from {transport} within {timeout} timeout");
    }
}
