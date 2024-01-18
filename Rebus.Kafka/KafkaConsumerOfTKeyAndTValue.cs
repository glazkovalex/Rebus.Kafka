using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Rebus.Kafka.Configs;
using Rebus.Kafka.Core;
using Rebus.Kafka.Dispatcher;
using Rebus.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
    /// <summary>Example message consumer</summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <typeparam name="TValue">Value type</typeparam>
    /// <remarks>If autocommiting is disabled, after processing the message, you need to call: <code>consumer.Commit(consumeResult.TopicPartitionOffset);</code></remarks>
    public class KafkaConsumer<TKey, TValue> : IDisposable
    {
        /// <summary>Get observable consumer for subscribe to incoming messages</summary>
        /// <param name="topics">Topics to subscribe to using the given message handler</param>
        /// <returns></returns>
        public IObservable<ConsumeResult<TKey, TValue>> Consume(IEnumerable<string> topics)
        {
            _topics = topics;
            ConsumerSubscibble(topics);
            var observable = Observable.Create<ConsumeResult<TKey, TValue>>((observer, token) =>
            {
                var task = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            while (!token.IsCancellationRequested)
                            {
                                ConsumeResult<TKey, TValue> consumeResult = _consumer.Consume(token);
                                if (consumeResult == null || consumeResult.IsPartitionEOF)
                                {
#if DEBUG
                                    _logger?.LogDebug($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}.");
#endif                                   
                                    continue;
                                }

                                observer.OnNext(consumeResult);
#if DEBUG
                                _logger?.LogDebug($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
#endif
                                if (consumeResult.Offset % _behaviorConfig.CommitPeriod == 0)
                                {
                                    // The Commit method sends a "commit offsets" request to the Kafka
                                    // cluster and synchronously waits for the response. This is very
                                    // slow compared to the rate at which the consumer is capable of
                                    // consuming messages. A high performance application will typically
                                    // commit offsets relatively infrequently and be designed handle
                                    // duplicate messages in the event of failure.
                                    _consumer.Commit(consumeResult);
                                }
                            }
                        }
                        catch (OperationCanceledException e)
                        {
                            _logger?.LogWarning($"Consume warning: {e.Message}");
                            observer.OnError(e);
                        }
                        catch (ConsumeException e)
                        {
                            _logger?.LogError($"Consume error: {e.Error}");
                            observer.OnError(e);
                        }
                        finally
                        {
                            _consumer.Close();
                        }
                        observer.OnCompleted();
                    },
                    token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default);
                task.ConfigureAwait(false);
                return task;
            });
            return observable;
        }

        void ConsumerSubscibble(IEnumerable<string> topics)
        {
            if (topics == null || topics.Count() == 0)
                return;

            if (_config.AllowAutoCreateTopics == true)
            {
                if (string.IsNullOrEmpty(_config?.BootstrapServers))
                    throw new ArgumentException("BootstrapServers it shouldn't be null!");

                using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _config.BootstrapServers }).Build())
                {
                    var existingsTopics = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                        .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                        .Select(t => t.Topic);
                    var missingTopics = topics.Where(t => !existingsTopics.Contains(t));
                    if (missingTopics.Any())
                    {
                        try
                        {
                            adminClient.CreateTopicsAsync(missingTopics.Select(mt => new TopicSpecification { Name = mt, ReplicationFactor = 1, NumPartitions = 1 }),
                                new CreateTopicsOptions { ValidateOnly = false }).GetAwaiter().GetResult();
                            existingsTopics = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                                .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                                .Select(t => t.Topic);
                            missingTopics = topics.Where(t => !existingsTopics.Contains(t));
                            if (missingTopics.Any())
                            {
                                throw new ArgumentException($"Failed to create topics: \"{string.Join("\", \"", missingTopics)}\"!", nameof(topics));
                            }
                        }
                        catch (CreateTopicsException e)
                        {
                            _logger.LogError($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                            throw;
                        }
                    }
                }
            }
            _consumer.Subscribe(topics);
        }

        /// <summary>Commits an offset based on the topic/partition/offset of a ConsumeResult.</summary>
        /// <returns>The ConsumeResult instance used to determine the committed offset.</returns>
        /// <exception cref="KafkaException">Thrown if the request failed.</exception>
        /// <exception cref="TopicPartitionOffsetException">Thrown if any of the constituent results is in error.
        /// The entire result (which may contain constituent results that are not in error) is available via the 
        /// Confluent.Kafka.TopicPartitionOffsetException.Results property of the exception.</exception>
        public List<TopicPartitionOffset> Commit() => _consumer.Commit();

        /// <summary>Commits an offset based on the topic/partition/offset of a ConsumeResult.</summary>
        /// <param name="topicPartitionOffsets">The topic/partition offsets to commit.</param>
        /// <returns>The ConsumeResult instance used to determine the committed offset.</returns>
        /// <exception cref="KafkaException">Thrown if the request failed.</exception>
        /// <exception cref="TopicPartitionOffsetException">Thrown if any of the constituent results is in error.
        /// The entire result (which may contain constituent results that are not in error) is available via the 
        /// Confluent.Kafka.TopicPartitionOffsetException.Results property of the exception.</exception>
        public void Commit(params TopicPartitionOffset[] topicPartitionOffsets) => _consumer.Commit(topicPartitionOffsets);

        /// <summary>Internal Consumer</summary>
        public IConsumer<TKey, TValue> Consumer => _consumer;

        #region Скучное

        private readonly ConsumerBehaviorConfig _behaviorConfig = new ConsumerBehaviorConfig();
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ConsumerConfig _config;
        private readonly ILogger<KafkaConsumer<TKey,TValue>> _logger;
        private IEnumerable<string> _topics;

        private void OnLog(object sender, LogMessage logMessage)
        {
            if (!logMessage.Message.Contains("MessageSet size 0, error \"Success\"")) // To avoid seeing messages about empty reads
                _logger?.LogDebug(
                    "Consuming from Kafka. Client: '{client}', syslog level: '{logLevel}', message: '{logMessage}'.",
                    logMessage.Name,
                    logMessage.Level,
                    logMessage.Message);
        }

        private void OnError(IConsumer<TKey, TValue> sender, Error error)
        {
            if (!error.IsFatal)
                _logger?.LogWarning("Consumer error: {error}. No action required.", error);
            else
            {
                var values = sender.Assignment;
                _logger?.LogError(
                    "Fatal error consuming from Kafka. Topic/partition/offset: '{topic}/{partition}/{offset}'. Error: '{error}'.",
                    string.Join(",", values.Select(a => a.Topic)),
                    string.Join(",", values.Select(a => a.Partition.Value)),
                    string.Join(",", values.Select(sender.Position)),
                    error.Reason);
                throw new KafkaException(error);
            }
        }

        private void ConsumerOnPartitionsAssigned(IConsumer<TKey, TValue> sender, List<TopicPartition> partitions)
        {
            _logger?.LogInformation($"Assigned partitions: [{string.Join(", ", partitions.Select(p => $"Topic:\"{p.Topic}\" Partition:{p.Partition.Value}"))}]");
            // possibly override the default partition assignment behavior:
            // consumer.Assign(...) 
        }

        private void ConsumerOnPartitionsRevoked(IConsumer<TKey, TValue> sender, List<TopicPartitionOffset> partitionOffsets)
        {
            _logger?.LogInformation($"Revoked partitions: [{string.Join(", ", partitionOffsets.Select(p => $"Topic:\"{p.Topic}\" Partition:{p.Partition.Value}"))}]");
            // consumer.Unassign()
        }

        IDeserializer<T> GetDeserializerFor<T>()
        {
            switch (typeof(T))
            {
                case var type when type == typeof(Null):
                    return (IDeserializer<T>)Deserializers.Null;
                case var type when type == typeof(Ignore):
                    return (IDeserializer<T>)Deserializers.Ignore;
                case var type when type == typeof(byte[]):
                    return (IDeserializer<T>)Deserializers.ByteArray;
                case var type when type == typeof(string):
                    return (IDeserializer<T>)Deserializers.Utf8;
                case var type when type == typeof(long):
                    return (IDeserializer<T>)Deserializers.Int64;
                case var type when type == typeof(int):
                    return (IDeserializer<T>)Deserializers.Int32;
                case var type when type == typeof(double):
                    return (IDeserializer<T>)Deserializers.Double;
                case var type when type == typeof(float):
                    return (IDeserializer<T>)Deserializers.Single;
                default: throw new NotSupportedException(nameof(T));
            };
        }

        /// <summary>Creates new instance <see cref="KafkaConsumer"/>.</summary>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="groupId">Id of group</param>
        /// <param name="logger"></param>
        /// <param name="keyDeserializer">key deserializer</param>
        /// <param name="valueDeserializer">value deserializer</param>
        public KafkaConsumer(string brokerList, string groupId = null, ILogger<KafkaConsumer<TKey, TValue>> logger = null
            , IDeserializer<TKey> keyDeserializer = null, IDeserializer<TValue> valueDeserializer = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new NullReferenceException(nameof(brokerList));
            _config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                ApiVersionRequest = true,
                GroupId = !string.IsNullOrEmpty(groupId) ? groupId : Guid.NewGuid().ToString(),
                EnableAutoCommit = false,
                FetchWaitMaxMs = 5,
                FetchErrorBackoffMs = 5,
                QueuedMinMessages = 1000,
                SessionTimeoutMs = 6000,
                //StatisticsIntervalMs = 5000,
#if DEBUG
                Debug = "msg",
#endif
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnablePartitionEof = true,
                AllowAutoCreateTopics = true,
            };
            _config.Set("fetch.message.max.bytes", "10240");
            Type keyType = typeof(TKey);

            keyDeserializer = keyDeserializer ?? GetDeserializerFor<TKey>();
            valueDeserializer = valueDeserializer ?? GetDeserializerFor<TValue>();

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Serdes
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            _consumer = new ConsumerBuilder<TKey, TValue>(_config)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .SetLogHandler(OnLog)
                .SetErrorHandler(OnError)
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler(ConsumerOnPartitionsAssigned)
                .SetPartitionsRevokedHandler(ConsumerOnPartitionsRevoked)
                .Build();
        }

        /// <summary>Creates new instance <see cref="KafkaConsumer{TKey, TValue}"/>.</summary>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="logger"></param>
        /// <param name="groupId">Id of group</param>
        public KafkaConsumer(string brokerList, ILogger<KafkaConsumer<TKey,TValue>> logger = null, string groupId = null) 
            : this(brokerList, groupId, logger) { }

        /// <summary>
        /// Creates new instance <see cref="KafkaConsumer{TKey, TValue}"/>. Allows you to configure
        /// all the parameters of the consumer used in this transport.
        /// </summary>
        /// <param name="consumerConfig">
        /// A collection of librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        /// and parameters specific to this client (refer to: <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        /// At a minimum, 'bootstrap.servers' and 'group.id' must be specified.
        /// </param>
        /// <param name="logger"></param>
        /// <param name="keyDeserializer">key deserializer</param>
        /// <param name="valueDeserializer">value deserializer</param>
        public KafkaConsumer(ConsumerConfig consumerConfig, ILogger<KafkaConsumer<TKey, TValue>> logger = null
            , IDeserializer<TKey> keyDeserializer = null, IDeserializer<TValue> valueDeserializer = null)
        {
            _logger = logger;
            if (string.IsNullOrWhiteSpace(consumerConfig?.BootstrapServers))
                throw new NullReferenceException($"{nameof(consumerConfig)}.{nameof(consumerConfig.BootstrapServers)}");
            if (string.IsNullOrEmpty(consumerConfig.GroupId))
                consumerConfig.GroupId = Guid.NewGuid().ToString("N");

            keyDeserializer = keyDeserializer ?? GetDeserializerFor<TKey>();
            valueDeserializer = valueDeserializer ?? GetDeserializerFor<TValue>();

            _config = consumerConfig;
            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Serdes
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            _consumer = new ConsumerBuilder<TKey, TValue>(_config)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .SetLogHandler(OnLog)
                .SetErrorHandler(OnError)
                .SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler(ConsumerOnPartitionsAssigned)
                .SetPartitionsRevokedHandler(ConsumerOnPartitionsRevoked)
                .Build();
        }

        /// <summary>
        /// Creates new instance <see cref="KafkaConsumer{TKey, TValue}"/>. Allows you to configure
        /// Creates new instance <see cref="KafkaConsumer{TKey, TValue}"/>. Allows you to configure
        /// all the parameters and behavior of the consumer used in this transport.
        /// </summary>
        /// <param name="consumerAndBehaviorConfig">
        /// Contains behavior settings in the Behavior property in addition to a collection of librdkafka configuration parameters
        /// (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) and parameters specific to this client
        /// (refer to: <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        /// At a minimum, 'bootstrap.servers' and 'group.id' must be specified.
        /// </param>
        /// <param name="logger"></param>
        public KafkaConsumer(ConsumerAndBehaviorConfig consumerAndBehaviorConfig, ILogger<KafkaConsumer<TKey, TValue>> logger = null)
            : this((ConsumerConfig)consumerAndBehaviorConfig, logger)
        {
            _behaviorConfig = consumerAndBehaviorConfig.BehaviorConfig;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_consumer != null)
            {
                _consumer.Dispose();
            }
        }

        #endregion
    }
}
