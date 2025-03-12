using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.Kafka.SchemaRegistry
{
    public class SchemaRegistryProtobufSerializer : ISerializer
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly bool _autoRegisterSchemas;
        private readonly SubjectNameStrategy _subjectNameStrategy;
        private readonly ConcurrentDictionary<Type, object> _serializers = new ConcurrentDictionary<Type, object>();
        private readonly ConcurrentDictionary<Type, object> _deserializers = new ConcurrentDictionary<Type, object>();

        public SchemaRegistryProtobufSerializer(
            SchemaRegistryConfig config,
            bool autoRegisterSchemas = false,
            SubjectNameStrategy subjectNameStrategy = SubjectNameStrategy.Topic)
        {
            _schemaRegistryClient = new CachedSchemaRegistryClient(config);
            _autoRegisterSchemas = autoRegisterSchemas;
            _subjectNameStrategy = subjectNameStrategy;
        }

        public async Task<TransportMessage> Serialize(Message message)
        {
            var body = message.Body;
            var type = body.GetType();
            var subject = GetSubjectName(type);

            // We get a Protobuf serializer for the type
            var serializer = _serializers.GetOrAdd(type, t =>
                Activator.CreateInstance(
                    typeof(ProtobufSerializer<>).MakeGenericType(t),
                    _schemaRegistryClient,
                    new ProtobufSerializerConfig
                    {
                        AutoRegisterSchemas = _autoRegisterSchemas,
                        SubjectNameStrategy = _subjectNameStrategy
                    },
                    null
                )
            );

            var serializeMethod = serializer.GetType().GetMethod("SerializeAsync");
            var context = new SerializationContext(MessageComponentType.Value, subject);
            byte[] result = await (Task<byte[]>)serializeMethod.Invoke(serializer, new object[] { body, context });

            return new TransportMessage(
                message.Headers.Clone(),
                result
            );
        }

        public async Task<Message> Deserialize(TransportMessage transportMessage)
        {
            if (!transportMessage.Headers.TryGetValue(Messages.Headers.Type, out var typeName))
                throw new InvalidOperationException("Missing message type header.");

            var type = Type.GetType(typeName);
            var subject = GetSubjectName(type);

            // We get a Protobuf deserializer for the type
            var deserializer = _deserializers.GetOrAdd(type, t =>
                Activator.CreateInstance(
                    typeof(ProtobufDeserializer<>).MakeGenericType(t),
                    _schemaRegistryClient,
                    new ProtobufDeserializerConfig
                    {
                        SubjectNameStrategy = _subjectNameStrategy
                    },
                    null
                )
            );

            var deserializeMethod = deserializer.GetType().GetMethod("DeserializeAsync");
            var context = new SerializationContext(MessageComponentType.Value, subject);
            ReadOnlyMemory<byte> body = (ReadOnlyMemory<byte>)transportMessage.Body;
            var resultTask = (Task)deserializeMethod.Invoke(deserializer, new object[] { body, false, context });
            await resultTask;

            var result = resultTask.GetType().GetProperty("Result").GetValue(resultTask);
            return new Message(transportMessage.Headers.Clone(), result);
        }

        private string GetSubjectName(Type type)
        {
            switch (_subjectNameStrategy)
            {
                case SubjectNameStrategy.Topic:
                    return $"{type.FullName}-value";
                case SubjectNameStrategy.Record:
                    return type.FullName;
                default:
                    throw new NotSupportedException($"Strategy {_subjectNameStrategy} not supported.");
            }
        }
    }
}
