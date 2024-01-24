namespace IdempotentSaga.Messages
{
    public class SagaMessageFire : ISagaMessage
    {
        public Guid SagaInstanceId { get; set; }
    }
}
