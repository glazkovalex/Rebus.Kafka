using Rebus.Messages;
using Rebus.Pipeline;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Rebus.Kafka.SchemaRegistry
{
    /// <summary>
    /// It adds the "kafka-topic" and "kafka-key" header with the value as {type name}-{"rbs2-msgid"}.
    /// These headers are service headers and will be removed from the message during its subsequent processing.
    /// </summary>
    [StepDocumentation(@"It adds the ""kafka-topic"" and ""kafka-key"" header with the value as {type name}-{""rbs2-msgid""}.")]
    public class AddTempKafkaHeaderStep : IOutgoingStep
    {
        public async Task Process(OutgoingStepContext context, Func<Task> next)
        {
            var message = context.Load<Message>();
            var destinationAddress = context.Load<Pipeline.Send.DestinationAddresses>()?.FirstOrDefault();
            // These headers are service headers and will be removed from the message during its subsequent processing.
            if (destinationAddress != null)
            {
                message.Headers[KafkaHeaders.KafkaTopic] = destinationAddress;
            }
            if (message?.Body != null && !message.Headers.ContainsKey(KafkaHeaders.KafkaKey) && message.Headers?.TryGetValue(Headers.MessageId, out string messageId) == true)
            {
                message.Headers[KafkaHeaders.KafkaKey] = $"{message.Body.GetType().Name}-{messageId}";
            }
            await next();
        }
    }
}
