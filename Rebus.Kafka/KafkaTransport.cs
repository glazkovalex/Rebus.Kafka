using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Kafka.Serialization;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Threading;
using Rebus.Transport;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
	/// <summary>Implementation of Apache Kafka Transport for Rebus</summary>
	public class KafkaTransport : ITransport, IInitializable, IDisposable, ISubscriptionStorage
	{
		public void CreateQueue(string address)
		{
			try
			{
				_subscriptions.TryAdd(address, new[] { address });
				// auto create topics should be enabled
				_queueConsumer.Subscribe(_subscriptions.SelectMany(a => a.Value));
			}
			catch (Exception e)
			{
				throw new RebusApplicationException(e, $"Queue declaration for '{address}' failed");
			}
		}

		const int CommitPeriod = 5; // ToDo: Добавить в параметры

		public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
		{
			if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
			if (message == null) throw new ArgumentNullException(nameof(message));
			if (context == null) throw new ArgumentNullException(nameof(context));

			DeliveryResult<Ignore, TransportMessage> result = null;
			try
			{
				result = await _producer.ProduceAsync(destinationAddress, new Message<Ignore, TransportMessage> { Value = message });
			}
			catch (Exception ex)
			{
				_log?.Error(ex,
					"Error producing to Kafka. Topic/partition: '{topic}/{partition}'. Message: {message}'.",
					destinationAddress,
					result?.Partition.ToString() ?? "N/A",
					result?.Value.ToString() ?? "N/A");
				throw;
			}
		}

		public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
		{
			bool resume = false;
			ConsumeResult<Ignore, TransportMessage> consumeResult = null;
			do
			{
				try
				{
					consumeResult = _queueConsumer.Consume(cancellationToken);
					resume = consumeResult.IsPartitionEOF;
					if (resume)
					{
						//_logger?.LogInformation($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}.");
						continue;
					}

					if (consumeResult.Offset % CommitPeriod == 0)
					{
						// The Commit method sends a "commit offsets" request to the Kafka
						// cluster and synchronously waits for the response. This is very
						// slow compared to the rate at which the consumer is capable of
						// consuming messages. A high performance application will typically
						// commit offsets relatively infrequently and be designed handle
						// duplicate messages in the event of failure.
						_queueConsumer.Commit(consumeResult);
					}
				}
				catch (OperationCanceledException e)
				{
					_log?.Info($"Consume warning: {e.Message}");
					resume = false;
					consumeResult = null;
				}
				catch (ConsumeException e)
				{
					_log?.Error($"Consume error: {e.Error}");
				}
			} while (resume);
			return Task.FromResult(consumeResult?.Value);
		}

		/// <summary>Gets the input queue name for this transport</summary>
		public string Address { get; }

		/// <inheritdoc />
		public Task<string[]> GetSubscriberAddresses(string topic)
		{
			return Task.FromResult(new[] { $"{_magicSubscriptionPrefix}{ReplaceInvalidTopicCharacter(topic)}" });
		}

		public Task RegisterSubscriber(string topic, string subscriberAddress)
		{
			_subscriptions.TryAdd(topic, new[] { $"{_magicSubscriptionPrefix}{ReplaceInvalidTopicCharacter(topic)}" });
			var topics = _subscriptions.SelectMany(a => a.Value).ToArray();
			var tcs = new TaskCompletionSource<bool>();
			//CancellationTokenRegistration registration = _cancellationToken.Register(() => tcs.SetCanceled());
			_waitAssigned.TryAdd(topics, new KeyValuePair<string, TaskCompletionSource<bool>>(topic, tcs));
			_queueConsumer.Subscribe(topics);
			return tcs.Task;
		}

		public Task UnregisterSubscriber(string topic, string subscriberAddress)
		{
			var toUnregister = _subscriptions.SelectMany(a => a.Value).ToArray();
			_subscriptions.TryRemove(topic, out _);
			var topics = _subscriptions.SelectMany(a => a.Value);
			try
			{
				_queueConsumer.Commit(_cancellationToken);
			}
			catch (Exception) { /* ignored */ }

			var tcs = new TaskCompletionSource<bool>();
			//CancellationTokenRegistration registration = _cancellationToken.Register(() => tcs.SetCanceled());
			_waitRevoked.TryAdd(toUnregister, new KeyValuePair<string, TaskCompletionSource<bool>>(topic, tcs));
			if (topics.Any())
				_queueConsumer.Subscribe(topics);
			else
				_queueConsumer.Close();
			return tcs.Task;
		}

		/// <summary>Always returns true because Kafka topics and subscriptions are global</summary>
		public bool IsCentralized => true;

		/// <summary>Initializes the transport by ensuring that the input queue has been created</summary>
		public void Initialize()
		{
			var builder = new ProducerBuilder<Ignore, TransportMessage>(_producerConfig)
				.SetKeySerializer(new IgnoreSerializer())
				.SetValueSerializer(new TransportMessageSerializer())
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

			// Note: If a key or value deserializer is not set (as is the case below), the 
			// deserializer corresponding to the appropriate type from Confluent.Kafka.Serdes
			// will be used automatically (where available). The default deserializer for string
			// is UTF8. The default deserializer for Ignore returns null for all input data
			// (including non-null data).
			_queueConsumer = new ConsumerBuilder<Ignore, TransportMessage>(_consumerConfig)
				.SetKeyDeserializer(Deserializers.Ignore)
				.SetValueDeserializer(new TransportMessageDeserializer())
				.SetLogHandler(ConsumerOnLog)
				.SetErrorHandler(ConsumerOnError)
				.SetStatisticsHandler(ConsumerOnStatistics)
				.SetRebalanceHandler(ConsumerOnRebalance)
				.Build();
			_queueConsumer.Subscribe(_subscriptions.SelectMany(a => a.Value));
		}

		#region logging

		private void ProducerOnLog(Producer<Ignore, TransportMessage> sender, LogMessage logMessage)
			=> _log.Debug(
				"Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
				logMessage.Name,
				logMessage.Level,
				logMessage.Message);

		private void ProducerOnStatistics(Producer<Ignore, TransportMessage> sender, string json)
			=> _log.Info($"Producer statistics: {json}");

		private void ProducerOnError(Producer<Ignore, TransportMessage> sender, Error error)
			=> _log.Warn("Producer error: {error}. No action required.", error);

		private void ConsumerOnLog(Consumer<Ignore, TransportMessage> sender, LogMessage logMessage)
		{
			if (!logMessage.Message.Contains("MessageSet size 0, error \"Success\""))//Чтобы не видеть сообщений о пустых чтениях
				_log.Debug(
					"Consuming from Kafka. Client: '{client}', syslog level: '{logLevel}', message: '{logMessage}'.",
					logMessage.Name,
					logMessage.Level,
					logMessage.Message);
		}

		private void ConsumerOnStatistics(Consumer<Ignore, TransportMessage> sender, string json)
			=> Console.WriteLine($"Consumer statistics: {json}");

		private void ConsumerOnError(Consumer<Ignore, TransportMessage> sender, Error error)
		{
			if (!error.IsFatal)
				_log.Warn("Consumer error: {error}. No action required.", error);
			else
			{
				var values = sender.Position(sender.Assignment);
				_log.Error(
					"Fatal error consuming from Kafka. Topic/partition/offset: '{topic}/{partition}/{offset}'. Error: '{error}'.",
					string.Join(",", values.Select(a => a.Topic)),
					string.Join(",", values.Select(a => a.Partition)),
					string.Join(",", values.Select(a => a.Offset)),
					error.Reason);
				throw new KafkaException(error);
			}
		}

		private void ConsumerOnRebalance(IConsumer<Ignore, TransportMessage> sender, RebalanceEvent evnt)
		{
			if (evnt.IsAssignment)
			{
				_log.Debug($"Assigned partitions: [{string.Join(", ", evnt.Partitions)}]");
				if (_waitAssigned.Count > 0)
				{
					var topics = evnt.Partitions.Select(p => p.Topic);
					var key = _waitAssigned.Keys.FirstOrDefault(k => !k.Except(topics).Any());
					if (key != null)
					{
						_waitAssigned.TryRemove(key, out var task);
						task.Value.SetResult(true);
						_log.Info($"Subscribe on \"{task.Key}\"");
					}
				}
				// possibly override the default partition assignment behavior:
				// consumer.Assign(...) 
			}
			else
			{
				_log.Debug($"Revoked partitions: [{string.Join(", ", evnt.Partitions)}]");
				if (_waitRevoked.Count > 0)
				{
					var topics = evnt.Partitions.Select(p => p.Topic);
					var key = _waitRevoked.Keys.FirstOrDefault(k => !k.Except(topics).Any());
					if (key != null)
					{
						_waitRevoked.TryRemove(key, out var task);
						task.Value.SetResult(true);
						_log.Info($"Unsubscribe from \"{task.Key}\"");
					}
				}
				// consumer.Unassign()
			}
		}

		#endregion

		#region Скучное

		private string ReplaceInvalidTopicCharacter(string topic)
		{
			return _topicRegex.Replace(topic, "_");
		}

		readonly ILog _log;
		readonly IAsyncTaskFactory _asyncTaskFactory;

		private Producer<Ignore, TransportMessage> _producer;
		private readonly ProducerConfig _producerConfig;
		private Consumer<Ignore, TransportMessage> _queueConsumer;
		private readonly ConsumerConfig _consumerConfig;

		private readonly ConcurrentDictionary<string, string[]> _subscriptions;
		private readonly string _magicSubscriptionPrefix = "---Topic---.";
		private readonly Regex _topicRegex = new Regex("[^a-zA-Z0-9\\._\\-]+");
		readonly CancellationToken _cancellationToken;

		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitAssigned
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();
		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitRevoked
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();

		/// <summary>Creates new instance <see cref="KafkaTransport"/>. Performs a simplified
		/// configuration of the parameters of the manufacturer and the consumer used in this transport.</summary>
		/// <param name="rebusLoggerFactory"></param>
		/// <param name="asyncTaskFactory"></param>
		/// <param name="brokerList">Initial list of brokers as a CSV list of broker host or host:port.</param>
		/// <param name="inputQueueName">name of input queue</param>
		/// <param name="groupId">Id of group</param>
		/// <param name="cancellationToken"></param>
		public KafkaTransport(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList
			, string inputQueueName, string groupId = null, CancellationToken cancellationToken = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(brokerList))
				throw new NullReferenceException(nameof(brokerList));
			var maxNameLength = 249;
			if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
				throw new ArgumentException("Недопустимые символы или длинна топика (файла)", nameof(inputQueueName));
			if (inputQueueName.StartsWith(_magicSubscriptionPrefix))
				throw new ArgumentException($"Sorry, but the queue name '{inputQueueName}' cannot be used because it conflicts with Rebus' internally used 'magic subscription prefix': '{_magicSubscriptionPrefix}'. ");

			_subscriptions = new ConcurrentDictionary<string, string[]>();
			_subscriptions.TryAdd(inputQueueName, new[] { inputQueueName });
			Address = inputQueueName;
			_cancellationToken = cancellationToken;
			_log = rebusLoggerFactory.GetLogger<KafkaTransport>();
			_asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));

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

			_consumerConfig = new ConsumerConfig
			{
				BootstrapServers = brokerList,
				ApiVersionRequest = true,
				GroupId = !string.IsNullOrEmpty(groupId) ? groupId : Guid.NewGuid().ToString("N"),
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
				EnablePartitionEof = true
			};
			_consumerConfig.Set("fetch.message.max.bytes", "10240");
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
		public KafkaTransport(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList, string inputQueueName
			, ProducerConfig producerConfig, ConsumerConfig consumerConfig, CancellationToken cancellationToken = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(brokerList))
				throw new NullReferenceException(nameof(brokerList));
			var maxNameLength = 249;
			if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
				throw new ArgumentException("Недопустимые символы или длинна топика (файла)", nameof(inputQueueName));
			if (inputQueueName.StartsWith(_magicSubscriptionPrefix))
				throw new ArgumentException($"Sorry, but the queue name '{inputQueueName}' cannot be used because it conflicts with Rebus' internally used 'magic subscription prefix': '{_magicSubscriptionPrefix}'. ");
			_producerConfig = producerConfig ?? throw new NullReferenceException(nameof(producerConfig));
			_producerConfig.BootstrapServers = brokerList;
			_consumerConfig = consumerConfig ?? throw new NullReferenceException(nameof(consumerConfig));
			_cancellationToken = cancellationToken;
			_consumerConfig.BootstrapServers = brokerList;
			if (string.IsNullOrEmpty(_consumerConfig.GroupId))
				_consumerConfig.GroupId = Guid.NewGuid().ToString("N");

			_subscriptions = new ConcurrentDictionary<string, string[]>();
			_subscriptions.TryAdd(inputQueueName, new[] { inputQueueName });
			Address = inputQueueName;
			_log = rebusLoggerFactory.GetLogger<KafkaTransport>();
			_asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
		}

		public void Dispose()
		{
			// Because the tasks returned from ProduceAsync might not be finished, wait for all messages to be sent
			_producer?.Flush(TimeSpan.FromSeconds(5));
			_producer?.Dispose();
			try
			{
				_queueConsumer.Commit();
			}
			catch (Exception) { /* ignored */ }
			_queueConsumer?.Close();
			_queueConsumer?.Dispose();
		}

		#endregion
	}
}
