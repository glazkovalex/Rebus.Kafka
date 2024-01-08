using System.Collections.Concurrent;

namespace Rebus.Kafka.Dispatcher
{
    internal class MessageBlock : ConcurrentDictionary<string, ProcessedMessage> { }
}
