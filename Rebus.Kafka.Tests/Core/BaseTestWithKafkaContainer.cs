using Fount.Web.Tests.Common.Logger;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Testcontainers.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Core
{
    /// <summary>
    /// Base class for test classes that require capturing
    /// </summary>
    public abstract class BaseTestWithKafkaContainer : IAsyncLifetime
    {
        protected readonly KafkaContainer _kafkaContainer = new KafkaBuilder().WithImage("confluentinc/cp-kafka:7.0.1").Build();

        public async Task InitializeAsync()
        {
            Logger.LogInformation("Initialize kafka container");
            await _kafkaContainer.StartAsync();
            BootstrapServer = _kafkaContainer.GetBootstrapAddress();
        }

        public Task DisposeAsync()
        {
            Logger.LogInformation("Dispose kafka container");
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
