using Rebus.Retry.FailFast;
using System;

namespace Rebus.Kafka.Tests.ErrorHandling
{
    class CustomFailFastChecker : IFailFastChecker
    {
        public bool ShouldFailFast(string messageId, Exception exception)
        {
            return exception is InvalidOperationException;
        }
    }
}
