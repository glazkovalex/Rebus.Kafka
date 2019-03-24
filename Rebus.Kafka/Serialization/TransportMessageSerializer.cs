using Confluent.Kafka;
using Newtonsoft.Json;
using Rebus.Messages;
using System.Text;

namespace Rebus.Kafka.Serialization
{
    class TransportMessageSerializer : ISerializer<TransportMessage>
    {
        /// <inheritdoc />
        public byte[] Serialize(TransportMessage data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
        }
    }
}
