using System.Collections.Generic;

namespace Rebus.Kafka.SchemaRegistry
{
    /// <summary>
    /// The header for setting the Apache Kafka message key value.
    /// </summary>
    public static class KafkaHeaders
    {
        /// <summary>
        /// Creates a header for specifying the key value of an Apache Kafka message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="key">The values of the Apache Kafka message key.</param>
        /// <returns></returns>
        public static new Dictionary<string, string> Create<TMessage, TKey>(TMessage message, TKey key)
        {
            string name = typeof(TMessage).Name;
            return new Dictionary<string, string> { { KafkaKey, $"{name}-{key.ToString()}" } };
        }

        /// <summary>
        /// The name of the header for specifying the Apache Kafka message key.
        /// </summary>
        public const string KafkaKey = "kafka-key";
        public const string KafkaTopic = "kafka-topic";
        public const string ValueSchemaId = "value.schema.id";
    }
}
