using Rebus.Kafka.NTests.Implementations;
using Rebus.Tests.Contracts.Transports;
using Rebus.Transport;
using System.Dynamic;
using System.Reflection;

namespace Rebus.Kafka.NTests.Transports
{
    [Ignore("It's not working yet")]
    [TestFixture, FixtureLifeCycle(LifeCycle.SingleInstance)]
    internal class SendReceiveTests : BasicSendReceive<KafkaTransportFactory>
    {
        [SetUp]
        public async Task Setup()
        {
            KafkaTransportFactory.initialization(SetUpTestWithKafkaContainer.BootstrapServer);
            _factory = new KafkaTransportFactory();
            _cancellationToken = new CancellationTokenSource().Token;
            Console.WriteLine("Setuped");
        }

        [Test]
        public async Task EmptyQueueReturnsNull_()
        {
            dynamic param = new ExpandoObject();
            param.Id = 12;
            param.Name = "Djon";
            param.Date = DateTime.UtcNow;
            string json = System.Text.Json.JsonSerializer.Serialize(param);

            var emptyQueue = _factory.Create(Rebus.Tests.Contracts.TestConfig.GetName("empty"));
            MethodInfo dynMethod = typeof(BasicSendReceive<KafkaTransportFactory>).GetMethod("WithContext", BindingFlags.NonPublic | BindingFlags.Static);
            Func<ITransactionContext, Task> func = async (context) =>
            {
                var transportMessage = await emptyQueue.Receive(context, CancellationToken.None);

                Assert.That(transportMessage, Is.Null);
            };
            await (Task)dynMethod.Invoke(null, new object[] { func, true });
        }

        #region uninteresting

        private ITransportFactory _factory;
        private CancellationToken _cancellationToken;

        //[OneTimeSetUp]
        //public async Task Init()
        //{
        //    Console.WriteLine("Initialize kafka container");
        //    await _container.StartAsync();
        //    BootstrapServer = _container.GetBootstrapAddress();
        //    KafkaTransportFactory.initialization(BootstrapServer);
        //}

        //[OneTimeTearDown]
        //public async Task Cleanup()
        //{
        //    Console.WriteLine("Dispose kafka container");
        //    await _container.DisposeAsync();
        //}

        //protected string BootstrapServer;
        //private readonly KafkaContainer _container;

        //public SendReceiveTests()
        //{
        //    Console.WriteLine("SendReceiveTests");
        //    _container = new KafkaBuilder().WithImage(BaseTestWithKafkaContainer.IMAGE_NAME).Build();
        //}

        #endregion
    }
}
