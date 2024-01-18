using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading;

namespace Rebus.Kafka
{
    /// <summary lang="en-US">Example message producer</summary>
    /// <summary lang="ru-RU">Пример поставщика сообщений</summary>
    public class KafkaProducer<TKey,TValue> : IDisposable
    {
        private readonly ILogger<KafkaProducer<TKey,TValue>> _logger;
        private readonly IProducer<TKey,TValue> _producer;

        /// <summary>Creates new instance <see cref="KafkaProducer{TKey,TValue}"/>.</summary>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="logger"></param>
        /// <param name="keySerializer">key serializer</param>
        /// <param name="valueSerializer">value serializer</param>
        public KafkaProducer(string brokerList, ILogger<KafkaProducer<TKey,TValue>> logger = null
            , ISerializer<TKey> keySerializer = null, ISerializer<TValue> valueSerializer = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new NullReferenceException(nameof(brokerList));
            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ApiVersionRequest = true,
                //QueueBufferingMaxKbytes = 10240,
                //{ "socket.blocking.max.ms", 1 }, // **DEPRECATED * *No longer used.
#if DEBUG
                Debug = "msg",
#endif
                //MessageTimeoutMs = 3000,
                BrokerVersionFallback = "0.10",
                ApiVersionFallbackMs = 0,
                //Acks = Acks.All,
            };
            //config.Set("queue.buffering.max.ms", "5");

            keySerializer = keySerializer ?? GetSerializerFor<TKey>();
            valueSerializer = valueSerializer ?? GetSerializerFor<TValue>();

            _producer = new ProducerBuilder<TKey,TValue>(config)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .SetLogHandler(OnLog)
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetErrorHandler(OnError)
                .Build();
        }

    /// <summary>Creates new instance <see cref="KafkaProducer{TKey,TValue}"/>. Allows you to configure
    /// all the parameters of the producer.</summary>
    /// <param name="producerConfig">A collection of librdkafka configuration parameters
    ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    ///     and parameters specific to this client (refer to:
    ///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
    ///     At a minimum, 'bootstrap.servers' must be specified.</param>
    /// <param name="logger"></param>
    public KafkaProducer(ProducerConfig producerConfig, ILogger<KafkaProducer<TKey,TValue>> logger = null
        , ISerializer<TKey> keySerializer = null, ISerializer<TValue> valueSerializer = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(producerConfig?.BootstrapServers))
                throw new NullReferenceException($"{nameof(producerConfig)}.{nameof(producerConfig.BootstrapServers)}");

            keySerializer = keySerializer ?? GetSerializerFor<TKey>();
            valueSerializer = valueSerializer ?? GetSerializerFor<TValue>();

            _producer = new ProducerBuilder<TKey, TValue>(producerConfig)
                .SetKeySerializer(keySerializer)
                .SetValueSerializer(valueSerializer)
                .SetLogHandler(OnLog)
                .SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .SetErrorHandler(OnError)
                .Build();
        }

    /// <summary>Creates new instance <see cref="KafkaProducer{TKey,TValue}"/> based on the Another <see cref="KafkaProducer{TKey,TValue}"/> instance for take
    /// the underlying librdkafka client handle that the Producer will use to make broker requests.</summary>
    /// <param name="dependentKafkaProducer">Another <see cref="KafkaProducer{TKey,TValue}"/> instance to take the connection from.</param>
    public KafkaProducer(KafkaProducer<TKey,TValue> dependentKafkaProducer)
        {
            if (dependentKafkaProducer == null)
                throw new ArgumentNullException(nameof(dependentKafkaProducer));
            var dependentProducer = typeof(KafkaProducer<TKey,TValue>).GetField(nameof(_producer), BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(dependentKafkaProducer) as IProducer<TKey,TValue>;
            _producer = new DependentProducerBuilder<TKey,TValue>(dependentProducer.Handle).Build();
            _logger = typeof(KafkaProducer<TKey,TValue>).GetField(nameof(_logger), BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(dependentKafkaProducer) as ILogger<KafkaProducer<TKey,TValue>>;
        }

        /// <summary>Send a message to a specific topic</summary>
        /// <param name="topic">Target topic</param>
        /// <param name="value">Message to send</param>
        /// <param name="cancellationToken">Token</param>
        /// <returns>Send result</returns>
        public async Task<DeliveryResult<TKey,TValue>> ProduceAsync(string topic, Message<TKey,TValue> value
            , CancellationToken cancellationToken = default(CancellationToken))
        {
            DeliveryResult<TKey,TValue> result = null;
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
                    result?.Value.ToString() ?? "N/A");
                throw;
            }
        }

        ISerializer<T> GetSerializerFor<T>()
        {
            switch (typeof(T))
            {
                case var type when type == typeof(Null):
                    return (ISerializer<T>)Serializers.Null;
                case var type when type == typeof(byte[]):
                    return (ISerializer<T>)Serializers.ByteArray;
                case var type when type == typeof(string):
                    return (ISerializer<T>)Serializers.Utf8;
                case var type when type == typeof(long):
                    return (ISerializer<T>)Serializers.Int64;
                case var type when type == typeof(int):
                    return (ISerializer<T>)Serializers.Int32;
                case var type when type == typeof(double):
                    return (ISerializer<T>)Serializers.Double;
                case var type when type == typeof(float):
                    return (ISerializer<T>)Serializers.Single;
                default: throw new NotSupportedException(nameof(T));
            };
        }

        /// <inheritdoc />
		public void Dispose()
        {
            // Because the tasks returned from ProduceAsync might not be finished, wait for all messages to be sent
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
        }

        private void OnLog(IProducer<TKey,TValue> sender, LogMessage logMessage)
            => _logger?.LogDebug(
                "Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
                logMessage.Name,
                logMessage.Level,
                logMessage.Message);

        private void OnError(IProducer<TKey,TValue> sender, Error error)
            => _logger?.LogWarning("Producer error: {error}. No action required.", error);
    }
}
