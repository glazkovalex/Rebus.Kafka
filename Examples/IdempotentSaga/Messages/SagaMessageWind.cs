namespace IdempotentSaga.Messages
{
    public class SagaMessageWind : ISagaMessage
    {
        public Guid SagaInstanceId { get; set; }
    }
}
