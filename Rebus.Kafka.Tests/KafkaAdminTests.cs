using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Rebus.Kafka.Tests.Base;
using Rebus.Kafka.Tests.Core;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    [Collection("Serial")]
    public class KafkaAdminTests : BaseTestWithKafkaContainer
    {
        [Fact]
        public async Task CreateAndDelete()
        {
            KafkaAdmin admin = new KafkaAdmin(BootstrapServer
                , new TestLogger<KafkaAdmin>(new TestOutputLoggerFactory(Output) { MinLevel = Logging.LogLevel.Info }.GetLogger<KafkaProducer<Null, string>>()));
            TopicSpecification[] topicSpecifications = new[] {
                new TopicSpecification { Name = $"{nameof(KafkaAdminTests)}.{Guid.NewGuid()}", NumPartitions = 3 },
                new TopicSpecification { Name = $"{nameof(KafkaAdminTests)}.{Guid.NewGuid()}", NumPartitions = 2 } };
            var existings = await admin.CreateTopicsAsync(topicSpecifications);
            Assert.Contains(existings, e => e.Topic == topicSpecifications[0].Name && e.Partitions.Count == topicSpecifications[0].NumPartitions);
            Assert.Contains(existings, e => e.Topic == topicSpecifications[1].Name && e.Partitions.Count == topicSpecifications[1].NumPartitions);

            existings = await admin.DeleteTopicsAsync(topicSpecifications.Select(t => t.Name));
            Assert.True(!existings.Any(e => e.Topic == topicSpecifications[0].Name && e.Partitions.Count == topicSpecifications[0].NumPartitions));
            Assert.True(!existings.Any(e => e.Topic == topicSpecifications[1].Name && e.Partitions.Count == topicSpecifications[1].NumPartitions));
        }

        public KafkaAdminTests(ITestOutputHelper output) : base(output) { }
    }
}
