using Confluent.Kafka;
using System.Text.Json;

namespace Rebus.Kafka.Extensions
{
    internal static class MessageOfTKeyAndTValueExtensions
    {
        public static string ToReadableText<TKey, TValue>(this Message<TKey, TValue> message)
        {
            if (message != null)
            {
                return JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true });
            }
            else
            {
                return "";
            }
        }
    }
}
