using Confluent.Kafka;
using Newtonsoft.Json;
using Rebus.Messages;
using System;
using System.Text;

namespace Rebus.Kafka.Serialization
{
	class TransportMessageDeserializer : IDeserializer<TransportMessage>
	{
		/// <inheritdoc />
		public TransportMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, bool isKey, MessageMetadata messageMetadata,
			TopicPartition source)
		{
			return JsonConvert.DeserializeObject<TransportMessage>(Encoding.UTF8.GetString(data.ToArray()));
		}
	}
}
