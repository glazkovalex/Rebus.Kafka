using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Retry;
using Rebus.Transport;

namespace Rebus.Kafka.Tests.ErrorHandling
{
    class CustomErrorHandler : IErrorHandler
    {
        public readonly ConcurrentQueue<TransportMessage> FailedMessages = new ConcurrentQueue<TransportMessage>();

        public async Task HandlePoisonMessage(TransportMessage transportMessage, ITransactionContext transactionContext, Exception exception)
        {
            FailedMessages.Enqueue(transportMessage);
        }
    }
}
