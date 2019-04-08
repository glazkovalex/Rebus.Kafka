
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Rebus.Kafka.Serialization
{
	class IgnoreSerializer : IAsyncSerializer<Ignore>
	{
		/// <inheritdoc />
		public Task<byte[]> SerializeAsync(Ignore data, SerializationContext context)
		{
			return Task.FromResult((byte[])null);
		}
	}
}
