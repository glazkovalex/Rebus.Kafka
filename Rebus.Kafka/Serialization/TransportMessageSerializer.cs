using Confluent.Kafka;
using Newtonsoft.Json;
using Rebus.Messages;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Kafka.Serialization
{
	class TransportMessageSerializer : IAsyncSerializer<TransportMessage>
	{

		/// <inheritdoc />
		public Task<byte[]> SerializeAsync(TransportMessage data, SerializationContext context)
		{
			return Task.FromResult(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data)));
		}
	}
}
