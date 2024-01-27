using Testcontainers.Kafka;

namespace Rebus.Kafka.NTests.Transports
{
    /// <summary>
    /// Base class for test classes that require capturing
    /// </summary>
    [SetUpFixture, FixtureLifeCycle(LifeCycle.SingleInstance)]
    public class SetUpTestWithKafkaContainer
    {
        [OneTimeSetUp]
        public async Task Init()
        {
            Console.WriteLine("Initialize kafka container");
            await _container.StartAsync();
            BootstrapServer = _container.GetBootstrapAddress();
        }

        [OneTimeTearDown]
        public async Task Cleanup()
        {
            Console.WriteLine("Dispose kafka container");
            await _container.DisposeAsync();
        }

        internal const string IMAGE_NAME = "confluentinc/cp-kafka:7.0.1";
        public static string BootstrapServer;
        private readonly KafkaContainer _container;

        public SetUpTestWithKafkaContainer()
        {
            Console.WriteLine("BaseTestWithKafkaContainer");
            _container = new KafkaBuilder().WithImage(IMAGE_NAME).Build();
        }
    }
}
