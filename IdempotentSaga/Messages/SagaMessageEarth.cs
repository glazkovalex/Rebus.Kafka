namespace IdempotentSaga.Messages
{
    public class SagaMessageEarth : ISagaMessage
    {
        public Guid SagaInstanceId { get; set; }
    }
}
