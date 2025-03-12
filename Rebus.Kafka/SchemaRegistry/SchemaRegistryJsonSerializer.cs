using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Rebus.Bus;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.Kafka.SchemaRegistry
{
    public class SchemaRegistryJsonSerializer : ISerializer
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _autoRegisterSchemas;
        private readonly string _inputQueueName;
        private readonly ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object> _serializers = new ConcurrentDictionary<(Type, SubjectNameStrategy), object>();
        private readonly ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object> _deserializers = new ConcurrentDictionary<(Type, SubjectNameStrategy), object>();

        public SchemaRegistryJsonSerializer(
            SchemaRegistryConfig config,
            string inputQueueName,
            bool autoRegisterSchemas = false)
        {
            _schemaRegistryClient = new CachedSchemaRegistryClient(config);
            _autoRegisterSchemas = autoRegisterSchemas;
            _inputQueueName = inputQueueName;
        }

        public async Task<TransportMessage> Serialize(Message message)
        {
            var body = message.Body;
            var type = body.GetType();
            var strategy = GetStrategy(message.Headers);

            // Getting or creating a JSON serializer for the type
            var serializer = _serializers.GetOrAdd((type, strategy), key =>
                Activator.CreateInstance(
                    typeof(JsonSerializer<>).MakeGenericType(key.type),
                    _schemaRegistryClient,
                    new JsonSerializerConfig
                    {
                        AutoRegisterSchemas = _autoRegisterSchemas,
                        SubjectNameStrategy = key.strategy,
                    },null,null
                )
            );
            var subject = PullSubjectName(message.Headers, strategy);
            var serializeMethod = serializer.GetType().GetMethod("SerializeAsync");
            var context = new SerializationContext(MessageComponentType.Value, subject);
            byte[] serializedData = await (Task<byte[]>)serializeMethod.Invoke(serializer, new object[] { body, context });
            if (_autoRegisterSchemas)
            {
                // Extracting the schema ID from binary data and adding the schema ID to the headers
                message.Headers[KafkaHeaders.ValueSchemaId] = ExtractSchemaId(serializedData);
            }
            return new TransportMessage(message.Headers.Clone(), serializedData);
        }

        public async Task<Message> Deserialize(TransportMessage transportMessage)
        {
            if (!transportMessage.Headers.TryGetValue(Messages.Headers.Type, out var typeName))
                throw new InvalidOperationException($"Missing \"{Messages.Headers.Type}\" header.");

            var strategy = GetStrategy(transportMessage.Headers);
            var type = Type.GetType(typeName);

            // Getting or creating a JSON deserializer for the type
            var deserializer = _deserializers.GetOrAdd((type, strategy), key =>
                Activator.CreateInstance(
                    typeof(JsonDeserializer<>).MakeGenericType(key.type),
                    _schemaRegistryClient,
                    new JsonDeserializerConfig
                    {
                        SubjectNameStrategy = key.strategy
                    },
                    null,
                    null
                )
            );

            var subject = GetSubjectName(transportMessage.Headers, strategy);
            var deserializeMethod = deserializer.GetType().GetMethod("DeserializeAsync");
            var context = new SerializationContext(MessageComponentType.Value, subject);
            ReadOnlyMemory<byte> body = (ReadOnlyMemory<byte>)transportMessage.Body;
            var resultTask = (Task)deserializeMethod.Invoke(deserializer, new object[] { body, false, context });
            await resultTask;

            var result = resultTask.GetType().GetProperty("Result").GetValue(resultTask);
            return new Message(transportMessage.Headers.Clone(), result);
        }

        private string ExtractSchemaId(byte[] data)
        {
            // Format Confluent: 1 byte (0x00) + 4 byte ID scheme (network byte order)
            if (data.Length < 5 || data[0] != 0x00)
                throw new InvalidDataException("Invalid Confluent wire format");

            var schemaIdBytes = new byte[4];
            Buffer.BlockCopy(data, 1, schemaIdBytes, 0, 4);

            if (BitConverter.IsLittleEndian)
                Array.Reverse(schemaIdBytes);

            return BitConverter.ToInt32(schemaIdBytes, 0).ToString();
        }

        private SubjectNameStrategy GetStrategy(Dictionary<string, string> headers)
        {
            if (headers == null)
                throw new ArgumentNullException(nameof(headers));
            if (!headers.TryGetValue(Messages.Headers.Intent, out var intent))
                throw new InvalidOperationException($"Missing \"{Messages.Headers.Intent}\" header.");

            switch (intent)
            {
                case Messages.Headers.IntentOptions.PublishSubscribe: return SubjectNameStrategy.Topic; // One scheme per <topic>-value
                case Messages.Headers.IntentOptions.PointToPoint: return SubjectNameStrategy.Record; // Multiple schemes per queue
                default: throw new NotSupportedException($"The \"{Messages.Headers.Intent}\" header with the value \"{intent}\" is not supported.");
            }
        }

        string GetSubjectName(IDictionary<string, string> headers, SubjectNameStrategy strategy)
        {
            switch (strategy)
            {
                case SubjectNameStrategy.Record:
                    if(_inputQueueName != null)
                    {
                        return _inputQueueName;
                    }
                    throw new InvalidOperationException($"If there is no {nameof(_inputQueueName)}, a unidirectional mode is expected, but a message deserialization request has been received!");
                case SubjectNameStrategy.Topic:
                    if (headers.TryGetValue(KafkaHeaders.KafkaTopic, out string topic))
                    {
                        return topic;
                    }
                    throw new NotSupportedException($"There is not enough header \"{KafkaHeaders.KafkaTopic}\" for serialization through the schema registry.");
                default: throw new NotSupportedException($"Not Supported {nameof(SubjectNameStrategy)}: {strategy}");
            }
        }

        private string PullSubjectName(IDictionary<string, string> headers, SubjectNameStrategy strategy)
        {
            if (headers == null)
                throw new ArgumentNullException(nameof(headers));
            if (headers.TryGetValue(KafkaHeaders.KafkaTopic, out string topic))
            {
                switch (strategy)
                {
                    case SubjectNameStrategy.Topic: // One scheme per topic
                        return topic;
                    case SubjectNameStrategy.Record: // Multiple schemes per queue
                        return $"{topic}-value";
                    default:
                        throw new NotSupportedException($"Strategy {strategy} not supported.");
                }
            }
            else
            {
                throw new NotSupportedException($"There is not enough header \"{KafkaHeaders.KafkaTopic}\" for serialization through the schema registry.");
            }
        }
    }
}
