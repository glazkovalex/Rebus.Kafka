using Confluent.Kafka;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Kafka.Core;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Transport;
using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Kafka.Configs;
using System.Collections.Generic;
using Rebus.Kafka.Extensions;
using System.Linq;

namespace Rebus.Kafka
{
    /// <summary>Implementation of Apache Kafka Transport for Rebus</summary>
    public class KafkaTransport : AbstractRebusTransport, IInitializable, IDisposable, ISubscriptionStorage
    {
        /// <inheritdoc />
        public override void CreateQueue(string address)
        {
            // one-way client does not create any queues
            if (Address == null || _queueSubscriptionStorage == null)
                return;
            _queueSubscriptionStorage.CreateQueues(address);
        }
        
        /// <summary>
        /// Sends all outgoing <see cref="TransportMessage"/> to the queue with the specified globally addressable name
        /// </summary>
        /// <exception cref="InvalidOperationException">If after waiting the procedure of transport initialization is still incomplete.</exception>
        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingTransportMessage> outgoingMessages, ITransactionContext context)
        {
            if (!(outgoingMessages?.Any() == true))
                return;

            if (context == null) throw new ArgumentNullException(nameof(context));

            if (_queueSubscriptionStorage?.IsInitialized == false) // waiting for initialization to complete
            {
                lock (_queueSubscriptionStorage)
                    if (_queueSubscriptionStorage?.IsInitialized == false)
                    {
                        const int waitSecont = 300; //5 minutes
                        int count = waitSecont * 10;
                        _log.Info($"Start waiting for the initialization to complete for {count / 600:N0} minutes...");
                        while (_queueSubscriptionStorage?.IsInitialized == false)
                        {
                            Thread.Sleep(100);
                            if (--count <= 0)
                                throw new InvalidOperationException(
                                    $"After waiting for {waitSecont / 60:N0} minutes, the procedure of transport initialization is still incomplete."
                                    + " There is no confirmation of completion of the subscription to the input queue."
                                    + " Try pausing before sending the first message, or handling this exception in a"
                                    + " loop to wait for the consumer's subscription to your queue to complete.");
                        }
                        _log.Info("The transport initialization is complete.");
                    }
            }

            DeliveryResult<string, byte[]> result = null;
            foreach (var outgoingMessage in outgoingMessages)
            {
                try
                {
                    var headers = new Confluent.Kafka.Headers();
                    foreach (var header in outgoingMessage.TransportMessage.Headers)
                    {
                        headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
                    }
                    var message = new Message<string, byte[]> { Value = outgoingMessage.TransportMessage.Body, Headers = headers/*, Timestamp = new Timestamp(DateTime.UtcNow)*/ };
                    result = await _producer.ProduceAsync(outgoingMessage.DestinationAddress, message);
                    if (result.Status == PersistenceStatus.NotPersisted)
                    {
                        throw new InvalidOperationException($"The message could not be sent. Try to resend the message: {message.ToReadableText()}");
                    }
#if DEBUG
                    _log.Debug($"The following message was sent to the topic \"{outgoingMessage.DestinationAddress}\": {System.Text.Json.JsonSerializer.Serialize(message)}");
#endif
                }
                catch (Exception ex)
                {
                    _log?.Error(ex,
                        "Error producing to Kafka. Topic/partition: '{topic}/{partition}'. Key: {key}; Value: {value}'.",
                        outgoingMessage.DestinationAddress,
                        result?.Partition.ToString() ?? "N/A",
                        result?.Key ?? "N/A",
                        result?.Value.ToString() ?? "N/A");
                    throw;
                }
            }
        }

        /// <inheritdoc />
        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            if (Address == null)
                throw new InvalidOperationException("This Kafka transport does not have an input queue - therefore, it is not possible to receive anything");
            try
            {
                TransportMessage receivedMessage = await _queueSubscriptionStorage.Receive(context, cancellationToken).ConfigureAwait(false);
                context.OnAck(tc =>
                {
                    _log.Debug($"context.OnAck : {receivedMessage.ToReadableText()}");
                    _queueSubscriptionStorage.Ack(receivedMessage);
                    return Task.CompletedTask; // Тут помечть сообщение обработанным и проверять не пора ли комитить самую старую порцию сообщений и комитить если пора _consumer.Commit...
                });
                context.OnNack(tc =>
                {
                    _log.Debug($"context.OnNack : {receivedMessage.ToReadableText()}");
                    _queueSubscriptionStorage.Nack(receivedMessage);
                    return Task.CompletedTask;
                });
                return receivedMessage;
            }
            catch (Exception exception)
            {
                Thread.Sleep(1000);
                throw new RebusApplicationException(exception,
                    $"Unexpected exception thrown while trying to dequeue a message from Kafka, queue address: {Address}");
            }
        }

        /// <inheritdoc />
        Task<IReadOnlyList<string>> ISubscriptionStorage.GetSubscriberAddresses(string topic)
        {
            return _queueSubscriptionStorage.GetSubscriberAddresses(topic);
        }

        /// <inheritdoc />
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            if (_queueSubscriptionStorage == null)
                throw new NotSupportedException("This Kafka transport don't support subscribing because he's a one-way Client.");
            await _queueSubscriptionStorage.RegisterSubscriber(topic, subscriberAddress).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            if (_queueSubscriptionStorage == null)
                throw new NotSupportedException("This Kafka transport don't support subscribing because he's a one-way Client.");
            await _queueSubscriptionStorage.UnregisterSubscriber(topic, subscriberAddress).ConfigureAwait(false);
        }

        /// <summary>Always returns true because Kafka topics and subscriptions are global</summary>
        public bool IsCentralized { get; } = true;

        /// <summary>Initializes the transport by ensuring that the input queue has been created</summary>
        public void Initialize()
        {
            _log.Info($"Initializing Kafka transport with queue \"{Address}\"");
            var builder = new ProducerBuilder<string, byte[]>(_producerConfig)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(Serializers.ByteArray)
                .SetLogHandler(ProducerOnLog)
                .SetStatisticsHandler(ProducerOnStatistics)
                .SetErrorHandler(ProducerOnError);
            try
            {
                _producer = builder.Build();
            }
            catch (DllNotFoundException)
            {   // Try loading librdkafka.dll
                if (!Library.IsLoaded)
                {
                    string directory = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().GetName().CodeBase.Substring(8));
                    var pathToLibrd = System.IO.Path.Combine(directory, $"librdkafka\\{(Environment.Is64BitOperatingSystem ? "x64" : "x86")}\\librdkafka.dll");
                    _log.Info($"librdkafka is not loaded. Trying to load {pathToLibrd}");
                    Library.Load(pathToLibrd);
                    _log.Info($"Using librdkafka version: {Library.Version}");
                }
                _producer = builder.Build();
            }

            _queueSubscriptionStorage?.Initialize();
        }

        #region logging

        private void ProducerOnLog(IProducer<string, byte[]> sender, LogMessage logMessage)
            => _log.Debug("Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
                logMessage.Name, logMessage.Level, logMessage.Message);

        private void ProducerOnStatistics(IProducer<string, byte[]> sender, string json)
            => _log.Info($"Producer statistics: {json}");

        private void ProducerOnError(IProducer<string, byte[]> sender, Error error)
            => _log.Warn("Producer error: {error}. No action required.", error);

        #endregion

        #region Скучное

        private string ReplaceInvalidTopicCharacter(string topic)
        {
            return _topicRegex.Replace(topic, "_");
        }
        /// <summary>For run repetitive background tasks</summary>
        readonly IAsyncTaskFactory _asyncTaskFactory;
        readonly ILog _log;
        private IProducer<string, byte[]> _producer;
        private readonly ProducerConfig _producerConfig;
        private readonly Regex _topicRegex = new Regex("[^a-zA-Z0-9\\._\\-]+");
        private readonly KafkaSubscriptionStorage _queueSubscriptionStorage;

        /// <summary>Creates new instance <see cref="KafkaTransport"/>. Performs a simplified
        /// configuration of the parameters of the manufacturer and the consumer used in this transport.</summary>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="inputQueueName">name of input queue</param>
        /// <param name="groupId">Id of group</param>
        /// <param name="cancellationToken"></param>
        public KafkaTransport(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList
            , string inputQueueName, string groupId = null) : base(inputQueueName)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new NullReferenceException(nameof(brokerList));

            _producerConfig = new ProducerConfig
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
            _producerConfig.Set("request.required.acks", "-1");
            _producerConfig.Set("queue.buffering.max.ms", "5");

            if (!string.IsNullOrWhiteSpace(inputQueueName)) // else OneWayClient
            {
                var maxNameLength = 249;
                if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
                    throw new ArgumentException("Invalid characters or the length of the topic (file)", nameof(inputQueueName));
                _queueSubscriptionStorage = new KafkaSubscriptionStorage(rebusLoggerFactory, asyncTaskFactory, brokerList, inputQueueName, groupId);
            } 

            _log = rebusLoggerFactory.GetLogger<KafkaTransport>();
            _asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
        }

        /// <summary>Creates new instance <see cref="KafkaTransport"/>. Allows you to configure
        /// all the parameters of the producer and the consumer used in this transport.</summary>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="inputQueueName">name of input queue</param>
        /// <param name="producerConfig">A collection of librdkafka configuration parameters
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to:
        ///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.</param>
        /// <param name="consumerConfig">A collection of librdkafka configuration parameters
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to:
        ///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.</param>
        /// <param name="cancellationToken"></param>
        public KafkaTransport(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList
            , string inputQueueName, ProducerConfig producerConfig, ConsumerConfig consumerConfig) : base(inputQueueName)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new NullReferenceException(nameof(brokerList));

            _producerConfig = producerConfig ?? throw new NullReferenceException(nameof(producerConfig));
            _producerConfig.BootstrapServers = brokerList;

            if (consumerConfig != null)
            {
                if (string.IsNullOrWhiteSpace(inputQueueName))
                    throw new ArgumentNullException(nameof(inputQueueName));
                var maxNameLength = 249;
                if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
                    throw new ArgumentException("Invalid characters or the length of the topic (file)", nameof(inputQueueName));
                if (consumerConfig is ConsumerAndBehaviorConfig consumerAndBehaviorConfig)
                {
                    _queueSubscriptionStorage = new KafkaSubscriptionStorage(rebusLoggerFactory, asyncTaskFactory, brokerList
                        , inputQueueName, consumerAndBehaviorConfig);
                }
                else
                {
                    _queueSubscriptionStorage = new KafkaSubscriptionStorage(rebusLoggerFactory, asyncTaskFactory, brokerList
                        , inputQueueName, consumerConfig);
                }
            }

            _log = rebusLoggerFactory.GetLogger<KafkaTransport>();
            _asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
        }

        /// <summary>Creates new instance <see cref="KafkaTransport"/>. Allows you to configure
        /// all the parameters and behavior of the producer and the consumer used in this transport.</summary>
        /// <param name="rebusLoggerFactory"></param>
        /// <param name="asyncTaskFactory"></param>
        /// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
        /// <param name="inputQueueName">name of input queue</param>
        /// <param name="producerConfig">A collection of librdkafka configuration parameters
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to:
        ///     <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.</param>
        /// <param name="consumerAndBehaviorConfig">
        /// Contains behavior settings in the Behavior property in addition to a collection of librdkafka configuration parameters
        /// (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) and parameters specific to this client
        /// (refer to: <see cref="T:Confluent.Kafka.ConfigPropertyNames" />).
        /// At a minimum, 'bootstrap.servers' and 'group.id' must be specified.
        /// </param>
        /// <param name="cancellationToken"></param>
        public KafkaTransport(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList
            , string inputQueueName, ProducerConfig producerConfig, ConsumerAndBehaviorConfig consumerAndBehaviorConfig)
            : this(rebusLoggerFactory, asyncTaskFactory, brokerList, inputQueueName, producerConfig, (ConsumerConfig)consumerAndBehaviorConfig) { }

        /// <inheritdoc />
        public void Dispose()
        {
            // Because the tasks returned from ProduceAsync might not be finished, wait for all messages to be sent
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
            _queueSubscriptionStorage?.Dispose();
        }

        #endregion
    }
}
