using Confluent.Kafka;

namespace Rebus.Kafka.Serialization
{
	class IgnoreSerializer : ISerializer<Ignore>
	{
		public byte[] Serialize(
			Confluent.Kafka.Ignore data,
			bool isKey,
			MessageMetadata messageMetadata,
			TopicPartition destination)
		{
			return null;
		}
	}
}
