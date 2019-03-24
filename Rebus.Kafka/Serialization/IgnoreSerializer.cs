
using Confluent.Kafka;

namespace Rebus.Kafka.Serialization
{
    class IgnoreSerializer : ISerializer<Ignore>
    {
        public byte[] Serialize(Confluent.Kafka.Ignore data, SerializationContext context)
        {
            return null;
        }
    }
}
