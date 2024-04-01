using Microsoft.Extensions.Logging;
using Rebus.Kafka.Tests.Core;
using System.Threading.Tasks;
using Testcontainers.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Base
{
    /// <summary>
    /// Base class for test classes that require capturing
    /// </summary>
    public abstract class BaseTestWithKafkaContainer : IAsyncLifetime
    {
        internal const string IMAGE_NAME = "confluentinc/cp-kafka:7.0.1";
        protected readonly KafkaContainer _kafkaContainer = new KafkaBuilder().WithImage(IMAGE_NAME).Build();

        public async Task InitializeAsync()
        {
            Logger.LogInformation("Initialize kafka container");
            await _kafkaContainer.StartAsync();
            BootstrapServer = _kafkaContainer.GetBootstrapAddress();
        }

        public Task DisposeAsync()
        {
            Logger.LogInformation("Dispose Kafka container");
            return _kafkaContainer.DisposeAsync().AsTask();
        }
        protected ITestOutputHelper Output;
        protected ILogger Logger;
        protected string BootstrapServer;

        protected BaseTestWithKafkaContainer(ITestOutputHelper output)
        {
            Output = output;
            Logger = XUnitLogger.CreateLogger<SimpleTests>(output);
        }
    }
}
