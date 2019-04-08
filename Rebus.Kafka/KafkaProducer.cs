using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
    /// <summary lang="en-US">Example message producer</summary>
    /// <summary lang="ru-RU">Пример поставщика сообщений</summary>
    public class KafkaProducer : IDisposable
    {
        private readonly ILogger<KafkaProducer> _logger;
        private readonly IProducer<Null, string> _producer;

        /// <summary>Creates new instance <see cref="KafkaProducer"/>.</summary>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="logger"></param>
        public KafkaProducer(string brokerList, ILogger<KafkaProducer> logger = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new NullReferenceException(nameof(brokerList));
            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ApiVersionRequest = true,
                QueueBufferingMaxKbytes = 10240,
                //{ "socket.blocking.max.ms", 1 }, // **DEPRECATED * *No longer used.
#if DEBUG
				Debug = "msg",
#endif
                MessageTimeoutMs = 3000,
            };
            config.Set("request.required.acks", "-1");
            config.Set("queue.buffering.max.ms", "5");

            _producer = new ProducerBuilder<Null, string>(config)
                .SetKeySerializer(Serializers.Null)
                .SetValueSerializer(Serializers.Utf8)
                .SetLogHandler(OnLog)
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetErrorHandler(OnError)
                .Build();
        }

        /// <summary>Creates new instance <see cref="KafkaProducer"/>. Allows you to configure
        /// all the parameters of the producer.</summary>
        /// <param name="producerConfig">A collection of librdkafka configuration parameters
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to:
        ///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.</param>
        /// <param name="logger"></param>
        public KafkaProducer(ProducerConfig producerConfig, ILogger<KafkaProducer> logger = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(producerConfig?.BootstrapServers))
                throw new NullReferenceException($"{nameof(producerConfig)}.{nameof(producerConfig.BootstrapServers)}");

            _producer = new ProducerBuilder<Null, string>(producerConfig)
                .SetKeySerializer(Serializers.Null)
                .SetValueSerializer(Serializers.Utf8)
                .SetLogHandler(OnLog)
                .SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .SetErrorHandler(OnError)
                .Build();
        }

        /// <summary>Creates new instance <see cref="KafkaProducer"/> based on the Another KafkaProducer instance for take
        /// the underlying librdkafka client handle that the Producer will use to make broker requests.</summary>
        /// <param name="dependentKafkaProducer">Another KafkaProducer instance to take the connection from.</param>
        public KafkaProducer(KafkaProducer dependentKafkaProducer)
        {
            if (dependentKafkaProducer == null)
                throw new ArgumentNullException(nameof(dependentKafkaProducer));
            var dependentProducer = typeof(KafkaProducer).GetField(nameof(_producer), BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(dependentKafkaProducer) as Producer<Null, string>;
            _producer = new DependentProducerBuilder<Null, string>(dependentProducer.Handle).Build();
            _logger = typeof(KafkaProducer).GetField(nameof(_logger), BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(dependentKafkaProducer) as ILogger<KafkaProducer>;
        }

        /// <summary>Send a message to a specific topic</summary>
        /// <param name="topic">Target topic</param>
        /// <param name="value">Message to send</param>
        /// <param name="cancellationToken">Token</param>
        /// <returns>Send result</returns>
        public async Task<DeliveryResult<Null, string>> ProduceAsync(string topic, Message<Null, string> value
            , CancellationToken cancellationToken = default(CancellationToken))
        {
            DeliveryResult<Null, string> result = null;
            try
            {
                result = await _producer.ProduceAsync(topic, value);
                //if (result.Error.HasError)
                //{
                //	throw new KafkaException(result.Error);
                //}
                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogError(
                    new EventId(),
                    ex,
                    "Error producing to Kafka. Topic/partition: '{topic}/{partition}'. Message: {message}'.",
                    topic,
                    result?.Partition.ToString() ?? "N/A",
                    result?.Value ?? "N/A");
                throw;
            }
        }

        /// <inheritdoc />
		public void Dispose()
        {
            // Because the tasks returned from ProduceAsync might not be finished, wait for all messages to be sent
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
        }

        private void OnLog(Producer<Null, string> sender, LogMessage logMessage)
            => _logger?.LogDebug(
                "Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
                logMessage.Name,
                logMessage.Level,
                logMessage.Message);

        private void OnError(Producer<Null, string> sender, Error error)
            => _logger?.LogWarning("Producer error: {error}. No action required.", error);
    }
}
