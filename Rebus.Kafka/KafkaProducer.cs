using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
	/// <summary>Example message producer</summary>
	public class KafkaProducer : IDisposable
	{
		private readonly ILogger _logger;
		private readonly Producer<Null, string> _producer;

		/// <summary>Creates new instance <see cref="KafkaProducer"/>.</summary>
		public KafkaProducer(ILogger logger, string brokerEndpoints)
		{
			_logger = logger;

			var config = new ProducerConfig
			{
				BootstrapServers = brokerEndpoints,
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

		/// <summary>Creates new instance <see cref="KafkaProducer"/> based on the connection settings in another producer.</summary>
		/// <param name="dependentProducer">Another producer instance to take the connection from</param>
		public KafkaProducer(Producer<Null, string> dependentProducer)
		{
			if (dependentProducer == null)
				throw new ArgumentNullException(nameof(dependentProducer));
			_producer = new DependentProducerBuilder<Null, string>(dependentProducer.Handle).Build();
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
				result = await _producer.ProduceAsync(topic, value, cancellationToken);
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

		public void Dispose()
		{
			// Because the tasks returned from ProduceAsync might not be finished, wait for all messages to be sent
			_producer.Flush(TimeSpan.FromSeconds(5));
			_producer?.Dispose();
		}

		private void OnLog(Producer<Null, string> sender, LogMessage logMessage)
			=> _logger?.LogInformation(
				"Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
				logMessage.Name,
				logMessage.Level,
				logMessage.Message);

		private void OnError(Producer<Null, string> sender, Error error)
			=> _logger?.LogInformation("Producer error: {error}. No action required.", error);
	}
}
