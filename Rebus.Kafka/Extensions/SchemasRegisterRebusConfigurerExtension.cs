using Rebus.Config;
using Confluent.SchemaRegistry;
using Rebus.Kafka.SchemaRegistry;
using Rebus.Transport;
using System;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;

namespace Rebus.Kafka
{
    public static class SchemaRegistryRebusConfigurerExtensions
    {
        /// <summary>
        /// Configures Rebus to use Confluent.SchemaRegistry.Json to serialize messages.
        /// Ensures that all messages are checked through the Confluent Schema Registry when they are sent and received.
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="schemaRegistryConfig"><see cref="SchemaRegistryConfig"></see></param>
        /// <param name="autoRegisterSchemas">Auto Register Schemas, not recommended for production!</param>
        /// <param name="subjectNameStrategy">Subject name strategy. Refer to: <see href="https://www.confluent.io/blog/put-several-event-types-kafka-topic/"></see></param>
        /// <returns></returns>
        public static RebusConfigurer UseSchemaRegistryJson(this RebusConfigurer configurer,
            SchemaRegistryConfig schemaRegistryConfig, bool autoRegisterSchemas = false)
        {
            configurer
                .Serialization(s =>
                {

                    s.Register(ctx =>
                    {
                        var inputQueueName = ctx.Get<ITransport>().Address;
                        return new SchemaRegistryJsonSerializer(schemaRegistryConfig, inputQueueName, autoRegisterSchemas);
                    });
                })
                .Options(oc =>
                {
                    oc.UseAttributeOrTypeFullNameForTopicNames();
                    oc.Decorate<IPipeline>(c =>
                    {
                        var pipeline = c.Get<IPipeline>();
                        return new PipelineStepInjector(pipeline)
                            .OnSend(new AddTempKafkaHeaderStep(), PipelineRelativePosition.Before, typeof(SerializeOutgoingMessageStep));
                    });
                });
            return configurer;
        }

        /// <summary>
        /// Configures Rebus to use Confluent.SchemaRegistry.Protobuf to serialize messages.
        /// Ensures that all messages are checked through the Confluent Schema Registry when they are sent and received.
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="schemaRegistryConfig"><see cref="SchemaRegistryConfig"></see></param>
        /// <param name="autoRegisterSchemas">Auto Register Schemas, not recommended for production!</param>
        /// <param name="subjectNameStrategy">Subject name strategy. Refer to: <see href="https://www.confluent.io/blog/put-several-event-types-kafka-topic/"></see></param>
        /// <returns></returns>
        [Obsolete("It's not working yet")]
        public static RebusConfigurer UseSchemaRegistryProtobuf(this RebusConfigurer configurer,
            SchemaRegistryConfig schemaRegistryConfig, bool autoRegisterSchemas = false, SubjectNameStrategy subjectNameStrategy = SubjectNameStrategy.Topic)
        {
            configurer.Serialization(s => s.Register(ctx =>
                new SchemaRegistryProtobufSerializer(schemaRegistryConfig, autoRegisterSchemas, subjectNameStrategy)));
            return configurer;
        }

        /// <summary>
        /// Configures Rebus to use Confluent.SchemaRegistry.Auro to serialize messages.
        /// Ensures that all messages are checked through the Confluent Schema Registry when they are sent and received.
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="schemaRegistryConfig"><see cref="SchemaRegistryConfig"></see></param>
        /// <param name="autoRegisterSchemas">Auto Register Schemas, not recommended for production!</param>
        /// <param name="subjectNameStrategy">Subject name strategy. Refer to: <see href="https://www.confluent.io/blog/put-several-event-types-kafka-topic/"></see></param>
        /// <returns></returns>
        [Obsolete("It's not working yet")]
        public static RebusConfigurer UseSchemaRegistryAvro(this RebusConfigurer configurer,
            SchemaRegistryConfig schemaRegistryConfig, bool autoRegisterSchemas = false, SubjectNameStrategy subjectNameStrategy = SubjectNameStrategy.Topic)
        {
            configurer.Serialization(s => s.Register(ctx =>
                new SchemaRegistryAvroSerializer(schemaRegistryConfig, autoRegisterSchemas, subjectNameStrategy)));
            return configurer;
        }

    }
}
