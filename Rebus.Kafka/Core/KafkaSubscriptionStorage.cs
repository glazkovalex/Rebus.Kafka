using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Rebus.Bus;
using Rebus.Kafka.Serialization;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
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
				_consumer.Commit(_cancellationToken);
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
				.SetRebalanceHandler(ConsumerOnRebalance)
				.Build();
			_consumer.Subscribe(_subscriptions.SelectMany(a => a.Value));
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
				_log.Info($"Assigned partitions: [{string.Join(", ", evnt.Partitions)}]");
				// possibly override the default partition assignment behavior:
				// consumer.Assign(...) 
			}
			else
			{
				_log.Info($"Revoked partitions: [{string.Join(", ", evnt.Partitions)}]");
				// consumer.Unassign()
			}
		}

		#endregion

		#region Скучное

		private string ReplaceInvalidTopicCharacter(string topic)
		{
			return _topicRegex.Replace(topic, "_");
		}

		private Consumer<Ignore, TransportMessage> _consumer;
		private ConsumerConfig _config;
		readonly ILog _log;
		private readonly ConcurrentDictionary<string, string[]> _subscriptions = new ConcurrentDictionary<string, string[]>();
		private readonly string _magicSubscriptionPrefix = "---Topic---.";
		private readonly Regex _topicRegex = new Regex("[^a-zA-Z0-9\\._\\-]+");
		readonly CancellationToken _cancellationToken;

		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitAssigned
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();
		readonly ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>> _waitRevoked
			= new ConcurrentDictionary<IEnumerable<string>, KeyValuePair<string, TaskCompletionSource<bool>>>();

		internal KafkaSubscriptionStorage(IRebusLoggerFactory rebusLoggerFactory, ConsumerConfig config
			, CancellationToken cancellationToken = default(CancellationToken))
		{
			_config = config ?? throw new NullReferenceException(nameof(config));
			if (string.IsNullOrWhiteSpace(_config.BootstrapServers))
				throw new NullReferenceException(nameof(_config.BootstrapServers));
			_config.GroupId = Guid.NewGuid().ToString("N");
			_cancellationToken = cancellationToken;
			_log = rebusLoggerFactory.GetLogger<KafkaSubscriptionStorage>();
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
			_consumer?.Dispose();
		}

		#endregion
	}
}
