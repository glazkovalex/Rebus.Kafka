using Confluent.Kafka;
using Newtonsoft.Json;
using Rebus.Messages;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Kafka.Serialization
{
	class TransportMessageDeserializer : IAsyncDeserializer<TransportMessage>
	{
		/// <inheritdoc />
		public Task<TransportMessage> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
		{
			return Task.FromResult(JsonConvert.DeserializeObject<TransportMessage>(Encoding.UTF8.GetString(data.ToArray())));
		}
	}
}
