namespace IdempotentSaga.Messages
{
    public interface ISagaMessage
    {
        public Guid SagaInstanceId { get; set; }
    }
}
