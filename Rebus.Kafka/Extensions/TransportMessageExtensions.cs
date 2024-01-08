using Rebus.Messages;
using System.Text.Json;
using Rebus.Extensions;

namespace Rebus.Kafka.Extensions
{
    internal static class TransportMessageExtensions
    {
        internal static string ToReadableText(this TransportMessage transportMessage)
        {
            if (transportMessage != null)
            {
                return JsonSerializer.Serialize(transportMessage, new JsonSerializerOptions { WriteIndented = true });
            }
            else
            {
                return "";
            }
        }

        internal static string GetId(this TransportMessage transportMessage)
        {
            if(transportMessage != null)
            {
                return transportMessage.Headers.GetValueOrNull(Headers.MessageId);
            }
            return null;
        }
    }
}
