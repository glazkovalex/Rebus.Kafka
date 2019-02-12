using Confluent.Kafka;
using Confluent.Kafka.Serdes;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Kafka
{
	/// <summary>Примерный производитель сообщений</summary>
	public class KafkaProducer : IDisposable
	{
		private readonly ILogger _logger;
		private readonly Producer<Null, string> _producer;

		/// <summary>Создает новый экземпляр <see cref="KafkaProducer"/>.</summary>
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

		/// <summary>Создает новый экземпляр <see cref="KafkaProducer"/> на основе подключения имеющегося в другом продюсере.</summary>
		/// <param name="dependentProducer">Другой продусер для извлечения подключения</param>
		public KafkaProducer(Producer<Null, string> dependentProducer)
		{
			if (dependentProducer == null)
				throw new ArgumentNullException(nameof(dependentProducer));
			_producer = new DependentProducerBuilder<Null, string>(dependentProducer.Handle).Build();
		}

		/// <summary>Отправить сообщение в указанный топик</summary>
		/// <param name="topic">Топик назначения</param>
		/// <param name="value">Сообщение</param>
		/// <param name="cancellationToken">Токен</param>
		/// <returns>Результат отправки</returns>
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
			// Так как задачи, возвращаемые ProduceAsync, могут не ожидаться, сообщения могут быть все еще в полете.
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
