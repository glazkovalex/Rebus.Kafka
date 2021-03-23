namespace Rebus.Kafka.Configs
{
    /// <summary>
    /// Consumer behavior configuration properties
    /// </summary>
    public class ConsumerBehaviorConfig
    {
        /// <summary>
        /// The period after which the commit offset will be set in Apache Kafka.
        /// <remarks>
        /// The Commit method sends a "commit offsets" request to the Kafka cluster and synchronously waits for the response.
        /// This is very slow compared to the rate at which the consumer is capable of consuming messages.
        /// A high performance application will typically commit offsets relatively infrequently and be designed
        /// handle duplicate messages in the event of failure.
        /// </remarks>
        /// </summary>
        public int CommitPeriod { get; set; } = 5;
    }
}
