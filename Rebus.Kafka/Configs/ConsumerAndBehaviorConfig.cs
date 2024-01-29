using System;
using Confluent.Kafka;

namespace Rebus.Kafka.Configs
{
    /// <summary>
    /// Consumer configuration properties with behavior 
    /// </summary>
    public class ConsumerAndBehaviorConfig : ConsumerConfig
    {
        /// <summary>
        /// Consumer behavior configuration properties
        /// </summary>
        public ConsumerBehaviorConfig BehaviorConfig { get; set; } = new ConsumerBehaviorConfig();

        /// <summary>Creates a new instance <see cref="ConsumerAndBehaviorConfig"/> with the default settings.</summary>
        public ConsumerAndBehaviorConfig() { }

        /// <summary>Creates a new instance <see cref="ConsumerAndBehaviorConfig"/> with the typical settings.</summary>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.</param>
        /// <param name="groupId">Client group id string. All clients sharing the same group.id belong to the same group.</param>
        public ConsumerAndBehaviorConfig(string brokerList, string groupId = null)
        {
            BootstrapServers = brokerList;
            ApiVersionRequest = true;
            GroupId = !string.IsNullOrEmpty(groupId) ? groupId : Guid.NewGuid().ToString("N");
            EnableAutoCommit = false;
            FetchWaitMaxMs = 5;
            FetchErrorBackoffMs = 5;
            QueuedMinMessages = 1000;
            SessionTimeoutMs = 6000;
            //StatisticsIntervalMs = 5000,
#if DEBUG
			TopicMetadataRefreshIntervalMs = 20000; // Otherwise it runs maybe five minutes
			Debug = "msg";
#endif
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest;
            EnablePartitionEof = true;
            AllowAutoCreateTopics = true;
            //MaxPollIntervalMs = 300000; Increase for long-term message processing
        }
    }
}
