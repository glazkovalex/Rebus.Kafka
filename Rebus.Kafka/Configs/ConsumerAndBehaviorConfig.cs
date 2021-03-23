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
    }
}
