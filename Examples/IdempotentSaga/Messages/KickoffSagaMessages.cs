using Rebus.Kafka;

namespace IdempotentSaga.Messages
{
    [Topic("IdempotentSaga.KickoffSagaEvent")]
    public class KickoffSagaMessages : ISagaMessage
    {
        public Guid SagaInstanceId { get; set; }
    }
}
