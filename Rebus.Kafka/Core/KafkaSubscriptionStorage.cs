using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Rebus.Bus;
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

namespace Rebus.Kafka.Core
{
	/// <summary>Implementation of Apache Kafka SubscriptionStorage</summary>
	public class KafkaSubscriptionStorage : ISubscriptionStorage, IInitializable, IDisposable
	{
		/// <summary>
		/// Receives the next message (if any) from the transport's input queue <see cref="P:Rebus.Transport.ITransport.Address" />
		/// </summary>
		internal Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
		{
			bool resume = false;
			ConsumeResult<Ignore, TransportMessage> consumeResult = null;
			do
			{
				try
				{
					consumeResult = _consumer.Consume(cancellationToken);
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
						_consumer.Commit(consumeResult);
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

		/// <inheritdoc />
		public Task<string[]> GetSubscriberAddresses(string topic)
		{
			return Task.FromResult(new[] { $"{_magicSubscriptionPrefix}{ReplaceInvalidTopicCharacter(topic)}" });
		}

		/// <inheritdoc />
		public Task RegisterSubscriber(string topic, string subscriberAddress)
		{
			_subscriptions.TryAdd(topic, new[] { $"{_magicSubscriptionPrefix}{ReplaceInvalidTopicCharacter(topic)}" });
			var topics = _subscriptions.SelectMany(a => a.Value).ToArray();
			var tcs = new TaskCompletionSource<bool>();
			//CancellationTokenRegistration registration = _cancellationToken.Register(() => tcs.SetCanceled());
			_waitAssigned.TryAdd(topics, new KeyValuePair<string, TaskCompletionSource<bool>>(topic, tcs));
			_consumer.Subscribe(topics);
			return tcs.Task;
		}

		/// <inheritdoc />
		public Task UnregisterSubscriber(string topic, string subscriberAddress)
		{
			var toUnregister = _subscriptions.SelectMany(a => a.Value).ToArray();
			_subscriptions.TryRemove(topic, out _);
			var topics = _subscriptions.SelectMany(a => a.Value);
			try
			{
				_consumer.Commit();
			}
			catch (Exception) { /* ignored */ }

			var tcs = new TaskCompletionSource<bool>();
			//CancellationTokenRegistration registration = _cancellationToken.Register(() => tcs.SetCanceled());
			_waitRevoked.TryAdd(toUnregister, new KeyValuePair<string, TaskCompletionSource<bool>>(topic, tcs));
			if (topics.Any())
				_consumer.Subscribe(topics);
			else
				_consumer.Close();
			return tcs.Task;
		}

		internal void CreateQueue(string address)
		{
			_subscriptions.TryAdd(address, new[] { address });
			_consumer.Subscribe(_subscriptions.SelectMany(a => a.Value));
		}

		/// <inheritdoc />
		public bool IsCentralized { get; } = true;

		/// <inheritdoc />
		public void Initialize()
		{
			// Note: If a key or value deserializer is not set (as is the case below), the 
			// deserializer corresponding to the appropriate type from Confluent.Kafka.Serdes
			// will be used automatically (where available). The default deserializer for string
			// is UTF8. The default deserializer for Ignore returns null for all input data
			// (including non-null data).
			_consumer = new ConsumerBuilder<Ignore, TransportMessage>(_config)
				.SetKeyDeserializer(Deserializers.Ignore)
				.SetValueDeserializer(new TransportMessageDeserializer())
				.SetLogHandler(ConsumerOnLog)
				.SetErrorHandler(ConsumerOnError)
				.SetStatisticsHandler(ConsumerOnStatistics)
				.SetPartitionsAssignedHandler((Action<IConsumer<Ignore, TransportMessage>, List<TopicPartition>>)ConsumerOnPartitionsAssigned)
				.SetPartitionsRevokedHandler((Action<IConsumer<Ignore, TransportMessage>, List<TopicPartitionOffset>>)ConsumerOnPartitionsRevoked)
				.Build();

			var topics = _subscriptions.SelectMany(a => a.Value).ToArray();
			var tcs = new TaskCompletionSource<bool>();
			_waitAssigned.TryAdd(topics, new KeyValuePair<string, TaskCompletionSource<bool>>(topics.First(), tcs));
			_consumer.Subscribe(topics);
			_initializationTask = tcs.Task;
		}

		internal bool IsInitialized => _initializationTask?.IsCompleted == true;
		private Task _initializationTask;

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
			=> _log.Info($"Consumer statistics: {json}");

		private void ConsumerOnError(Consumer<Ignore, TransportMessage> sender, Error error)
		{
			if (!error.IsFatal)
				_log.Warn("Consumer error: {error}. No action required.", error);
			else
			{
				var values = sender.Assignment;
				_log.Error(
					"Fatal error consuming from Kafka. Topic/partition/offset: '{topic}/{partition}/{offset}'. Error: '{error}'.",
					string.Join(",", values.Select(a => a.Topic)),
					string.Join(",", values.Select(a => a.Partition)),
					string.Join(",", values.Select(sender.Position)),
					error.Reason);
				throw new KafkaException(error);
			}
		}

		private void ConsumerOnPartitionsAssigned(IConsumer<Ignore, TransportMessage> sender, List<TopicPartition> partitions)
		{
			_log.Debug($"Assigned partitions: [{string.Join(", ", partitions.Select(p => p.Partition))}]");
			if (_waitAssigned.Count > 0)
			{
				var topics = partitions.Select(p => p.Topic).Distinct();
				var keys = _waitAssigned.Keys.Where(k => !k.Except(topics).Any());
				foreach (var key in keys)
				{
					_waitAssigned.TryRemove(key, out var task);
					task.Value.SetResult(true);
					_log.Info($"Subscribe on \"{task.Key}\"");
				}
			}
			// possibly override the default partition assignment behavior:
			// consumer.Assign(...) 
		}

		private void ConsumerOnPartitionsRevoked(IConsumer<Ignore, TransportMessage> sender, List<TopicPartitionOffset> partitionOffsets)
		{
			_log.Debug($"Revoked partitions: [{string.Join(", ", partitionOffsets.Select(po => po.Partition))}]");
			if (_waitRevoked.Count > 0)
			{
				var topics = partitionOffsets.Select(p => p.Topic).Distinct();
				var keys = _waitRevoked.Keys.Where(k => !k.Except(topics).Any());
				foreach (var key in keys)
				{
					_waitRevoked.TryRemove(key, out var task);
					task.Value.SetResult(true);
					_log.Info($"Unsubscribe from \"{task.Key}\"");
				}
			}
			// consumer.Unassign()
		}

		#endregion

		#region Скучное

		private string ReplaceInvalidTopicCharacter(string topic)
		{
			return _topicRegex.Replace(topic, "_");
		}

		const int CommitPeriod = 5; // ToDo: Добавить в параметры
		private IConsumer<Ignore, TransportMessage> _consumer;
		private readonly ConsumerConfig _config;
		readonly ILog _log;
		readonly IAsyncTaskFactory _asyncTaskFactory;

		private readonly ConcurrentDictionary<string, string[]> _subscriptions = new ConcurrentDictionary<string, string[]>();
		private readonly string _magicSubscriptionPrefix = "---Topic---.";
		private readonly Regex _topicRegex = new Regex("[^a-zA-Z0-9\\._\\-]+");
		readonly CancellationToken _cancellationToken;

		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitAssigned
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();
		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitRevoked
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();

		private readonly object _subscriptionLockObj = new object();

		internal KafkaSubscriptionStorage(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory
			, string brokerList, string inputQueueName, string groupId = null, CancellationToken cancellationToken = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(brokerList))
				throw new NullReferenceException(nameof(brokerList));
			var maxNameLength = 249;
			if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
				throw new ArgumentException("Недопустимые символы или длинна топика (файла)", nameof(inputQueueName));
			if (inputQueueName.StartsWith(_magicSubscriptionPrefix))
				throw new ArgumentException($"Sorry, but the queue name '{inputQueueName}' cannot be used because it conflicts with Rebus' internally used 'magic subscription prefix': '{_magicSubscriptionPrefix}'. ");

			_config = new ConsumerConfig
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
				TopicMetadataRefreshIntervalMs = 20000, // Otherwise it runs maybe five minutes
				Debug = "msg",
#endif
				AutoOffsetReset = AutoOffsetReset.Latest,
				EnablePartitionEof = true
			};
			_config.Set("fetch.message.max.bytes", "10240");

			_asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
			_log = rebusLoggerFactory.GetLogger<KafkaSubscriptionStorage>();
			_cancellationToken = cancellationToken;

			_subscriptions.TryAdd(inputQueueName, new[] { inputQueueName });
		}

		internal KafkaSubscriptionStorage(IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, string brokerList
			, string inputQueueName, ConsumerConfig config, CancellationToken cancellationToken = default(CancellationToken))
		{
			if (string.IsNullOrWhiteSpace(brokerList))
				throw new NullReferenceException(nameof(brokerList));
			var maxNameLength = 249;
			if (inputQueueName.Length > maxNameLength && _topicRegex.IsMatch(inputQueueName))
				throw new ArgumentException("Недопустимые символы или длинна топика (файла)", nameof(inputQueueName));
			if (inputQueueName.StartsWith(_magicSubscriptionPrefix))
				throw new ArgumentException($"Sorry, but the queue name '{inputQueueName}' cannot be used because it conflicts with Rebus' internally used 'magic subscription prefix': '{_magicSubscriptionPrefix}'. ");

			_config = config ?? throw new NullReferenceException(nameof(config));
			_config.BootstrapServers = brokerList;
			if (string.IsNullOrEmpty(_config.GroupId))
				_config.GroupId = Guid.NewGuid().ToString("N");

			_asyncTaskFactory = asyncTaskFactory ?? throw new ArgumentNullException(nameof(asyncTaskFactory));
			_log = rebusLoggerFactory.GetLogger<KafkaSubscriptionStorage>();
			_cancellationToken = cancellationToken;

			_subscriptions.TryAdd(inputQueueName, new[] { inputQueueName });
		}

		/// <inheritdoc />
		public void Dispose()
		{
			try
			{
				_consumer.Commit();
			}
			catch (Exception) { /* ignored */ }
			_consumer?.Close();
			_log.Info($"Closed consumer BootstrapServers:{_config.BootstrapServers}, gropId: {_config.GroupId}.");
			_consumer?.Dispose();
		}

		#endregion
	}
}
