using Confluent.Kafka;
using Rebus.Messages;

namespace Rebus.Kafka.Dispatcher
{
    internal class ProcessedMessage
    {
        internal TopicPartitionOffset TopicPartitionOffset { get; set; }
        internal MessageProcessingStatuses Status { get; set; }
        internal TransportMessage Message { get; set; }

        internal ProcessedMessage(TopicPartitionOffset topicPartitionOffset, MessageProcessingStatuses status, TransportMessage message = null)
        {
            TopicPartitionOffset = topicPartitionOffset;
            Status = status;
            Message = message;
        }
        public override string ToString() {
            return $"{Status};{(Status != MessageProcessingStatuses.Processing ? " " : "")} Topic:{TopicPartitionOffset.Topic}, Partition:{TopicPartitionOffset.Partition.Value}, Offset:{TopicPartitionOffset.Offset.Value}";
        }
    }

    internal enum MessageProcessingStatuses
    {
        Processing,
        Completed,
        Reprocess
    }
}
