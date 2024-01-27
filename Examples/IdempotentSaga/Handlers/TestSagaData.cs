using Rebus.Sagas.Idempotent;

namespace IdempotentSaga.Handlers
{
    public class TestSagaData : IIdempotentSagaData
    {
        public Guid SagaInstanceId { get; set; }
        public bool Task1Processed { get; set; }
        public bool Task2Processed { get; set; }
        public bool Task3Processed { get; set; }
        public string StuffDone { get; set; }
        public IdempotencyData IdempotencyData { get; set; }
        public Guid Id { get; set; }
        public int Revision { get; set; }
    }
}
