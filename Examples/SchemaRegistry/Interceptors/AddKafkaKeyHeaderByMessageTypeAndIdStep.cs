using Rebus.Kafka.SchemaRegistry;
using Rebus.Messages;
using Rebus.Pipeline;
using SchemaRegistry.Events;

namespace SchemaRegistry.Interceptors
{
    /// <summary>
    /// It adds the "kafka-key" header with the value as {type name}-{IWithId.Id}.
    /// </summary>
    [StepDocumentation("It adds the \"kafka-key\" header with the value as {type name}-{IWithId.Id}.")]
    public class AddKafkaKeyHeaderByMessageTypeAndIdStep : IOutgoingStep
    {
        public async Task Process(OutgoingStepContext context, Func<Task> next)
        {
            var message = context.Load<Message>();
            if (message?.Body == null)
                await next();

            if (message!.Body is IWithId body)
            {
                message.Headers[KafkaHeaders.KafkaKey] = $"{body.GetType().Name}-{body.Id.ToString()}";
            }
            await next();
        }
    }
}
