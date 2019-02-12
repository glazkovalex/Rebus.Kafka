using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
	/// <summary>Примерный потребитель сообщений</summary>
	public sealed class KafkaConsumer : IDisposable
	{
		private readonly ILogger _logger;
		private IEnumerable<string> _topics;
		private readonly Consumer<Null, string> _consumer;

		/// <summary>Создает новый экземпляр <see cref="KafkaConsumer"/>.</summary>
		public KafkaConsumer(ILogger logger, string brokerEndpoints, string groupId = null)
		{
			_logger = logger;

			var config = new ConsumerConfig
			{
				BootstrapServers = brokerEndpoints,
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
				EnablePartitionEof = true
			};
			config.Set("fetch.message.max.bytes", "10240");

			// Note: If a key or value deserializer is not set (as is the case below), the 
			// deserializer corresponding to the appropriate type from Confluent.Kafka.Serdes
			// will be used automatically (where available). The default deserializer for string
			// is UTF8. The default deserializer for Ignore returns null for all input data
			// (including non-null data).
			_consumer = new ConsumerBuilder<Null, string>(config)
				.SetKeyDeserializer(Deserializers.Null)
				.SetValueDeserializer(Deserializers.Utf8)
				.SetLogHandler(OnLog)
				.SetErrorHandler(OnError)
				.SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
				.SetRebalanceHandler(OnRebalance)
				.Build();
		}

		public Consumer<Null, string> Consumer => _consumer; 

		/// <summary>Поглощать входящие сообщения</summary>
		/// <param name="topics">Топики, из которых хочется поглощать</param>
		/// <param name="action">Обработчик входящих сообщений</param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task Consume(IEnumerable<string> topics, Action<Message<Null, string>> action, CancellationToken cancellationToken)
		{
			_topics = topics;
			const int commitPeriod = 5;

			Task task = Task.Factory.StartNew(() =>
				{
					_consumer.Subscribe(topics);
					while (!cancellationToken.IsCancellationRequested)
					{
						try
						{
							var consumeResult = _consumer.Consume(cancellationToken);
							if (consumeResult.IsPartitionEOF)
							{
								//_logger?.LogInformation($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}.");
								continue;
							}

							action(consumeResult.Message);
							//_logger?.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

							if (consumeResult.Offset % commitPeriod == 0)
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
						catch (ConsumeException e)
						{
							_logger?.LogError($"Consume error: {e.Error}");
						}
					}
					_consumer.Close();
				},
				cancellationToken,
				TaskCreationOptions.LongRunning,
				TaskScheduler.Default
			);
			task.ConfigureAwait(false);
			return task;
		}

		private readonly EventHandler<Message<Null, string>> _onMessage;

		//public async Task CommitAsync(Message<Null, string> message) => await _consumer.CommitAsync(message);

		public void Dispose()
		{
			if (_consumer != null)
			{
				_consumer.Unsubscribe();
				_consumer.Dispose();
			}
		}

		private void OnLog(object sender, LogMessage logMessage)
		{
			if (!logMessage.Message.Contains("MessageSet size 0, error \"Success\""))//Чтобы не видеть сообщений о пустых чтениях
				_logger?.LogInformation(
					"Consuming from Kafka. Client: '{client}', syslog level: '{logLevel}', message: '{logMessage}'.",
					logMessage.Name,
					logMessage.Level,
					logMessage.Message);
		}

		private void OnError(Consumer<Null, string> sender, Error error)
		{
			if (!error.IsFatal)
				_logger?.LogInformation("Consumer error: {error}. No action required.", error);
			else
			{
				var values = sender.Position(sender.Assignment);
				_logger?.LogError(
					"Fatal error consuming from Kafka. Topic/partition/offset: '{topic}/{partition}/{offset}'. Error: '{error}'.",
					string.Join(",", values.Select(a => a.Topic)),
					string.Join(",", values.Select(a => a.Partition)),
					string.Join(",", values.Select(a => a.Offset)),
					error.Reason);
				throw new KafkaException(error);
			}
		}

		private void OnRebalance(IConsumer<Null, string> sender, RebalanceEvent evnt)
		{
			if (evnt.IsAssignment)
			{
				_logger?.LogInformation($"Assigned partitions: [{string.Join(", ", evnt.Partitions)}]");
				// possibly override the default partition assignment behavior:
				// consumer.Assign(...) 
			}
			else
			{
				_logger?.LogInformation($"Revoked partitions: [{string.Join(", ", evnt.Partitions)}]");
				// consumer.Unassign()
			}
		}
	}
}
