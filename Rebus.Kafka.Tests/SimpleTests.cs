using Autofac;
using Confluent.Kafka;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.Routing.TypeBased;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
	public class SimpleTests
	{
		[Fact]
		public void SendReceive()
		{
			using (var adapter = new BuiltinHandlerActivator())
			{
				var amount = 0;
				Stopwatch sw = Stopwatch.StartNew();

				adapter.Handle<Message>(message =>
				{
					amount = amount + message.MessageNumber;
					_output.WriteLine($"Received : \"{message.MessageNumber}\"");
					if (message.MessageNumber == MessageCount)
						_output.WriteLine($"Получено {MessageCount} сообщений за {sw.ElapsedMilliseconds / 1000f:N3}с");
					return Task.CompletedTask;
				});

				Configure.With(adapter)
					.Logging(l => l.ColoredConsole(minLevel: LogLevel.Info))
					.Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), "temp"))
					.Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
					.Start();

				var sendAmount = 0;
				var messages = Enumerable.Range(1, MessageCount)
					.Select(i =>
					{
						sendAmount = sendAmount + i;
						return adapter.Bus.Send(new Message { MessageNumber = i });
					}).ToArray();

				Task.WaitAll(messages);
				Thread.Sleep(10000);

				Assert.Equal(amount, sendAmount);
			}
		}

		[Fact]
		public void PublishSubscribe()
		{
			IContainer container;
			var builder = new ContainerBuilder();
			builder.RegisterInstance(_output).As<ITestOutputHelper>().SingleInstance();
			builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(LogLevel.Info))
				.Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), "temp"))
				.Options(o => o.SetMaxParallelism(5))
			);

			using (container = builder.Build())
			using (IBus bus = container.Resolve<IBus>())
			{
				bus.Subscribe<Message>().Wait();

				var sendAmount = 0;
				bus.Publish(new Message { MessageNumber = 0 }).Wait();
				var messages = Enumerable.Range(1, MessageCount)
					.Select(i =>
				   {
					   sendAmount = sendAmount + i;
					   return bus.Publish(new Message { MessageNumber = i });
				   }).ToArray();

				Task.WaitAll(messages);
				Thread.Sleep(10000);

				Assert.Equal(Amount, sendAmount);
			}
		}

		[Fact]
		public void DetailedConfiguration()
		{
			using (var adapter = new BuiltinHandlerActivator())
			{
				var amount = 0;
				Stopwatch sw = Stopwatch.StartNew();

				adapter.Handle<Message>(message =>
				{
					amount = amount + message.MessageNumber;
					_output.WriteLine($"Received : \"{message.MessageNumber}\"");
					if (message.MessageNumber == MessageCount)
						_output.WriteLine($"Received {MessageCount} message for {sw.ElapsedMilliseconds / 1000f:N3}с");
					return Task.CompletedTask;
				});

				#region Setting producer and consumer

				var producerConfig = new ProducerConfig
				{
					//BootstrapServers = , //will be set from the general parameter
					ApiVersionRequest = true,
					QueueBufferingMaxKbytes = 10240,
					//{ "socket.blocking.max.ms", 1 }, // **DEPRECATED * *No longer used.
#if DEBUG
				Debug = "msg",
#endif
					MessageTimeoutMs = 3000,
				};
				producerConfig.Set("request.required.acks", "-1");
				producerConfig.Set("queue.buffering.max.ms", "5");

				var consumerConfig = new ConsumerConfig
				{
					//BootstrapServers = , //will be set from the general parameter
					ApiVersionRequest = true,
					GroupId = "temp",
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
				consumerConfig.Set("fetch.message.max.bytes", "10240");

				#endregion

				Configure.With(adapter)
					.Logging(l => l.ColoredConsole(minLevel: LogLevel.Info))
					.Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), producerConfig, consumerConfig))
					.Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
					.Start();

				var sendAmount = 0;
				var messages = Enumerable.Range(1, MessageCount)
					.Select(i =>
					{
						sendAmount = sendAmount + i;
						return adapter.Bus.Send(new Message { MessageNumber = i });
					}).ToArray();

				Task.WaitAll(messages);
				Thread.Sleep(10000);

				Assert.Equal(amount, sendAmount);
			}
		}

		#region Settings

		internal static int Amount;
		const int MessageCount = 10;
		private readonly ITestOutputHelper _output;
		static readonly string kafkaEndpoint = "192.168.0.166:9092";

		/// <summary>Creates new instance <see cref="SimpleTests"/>.</summary>
		public SimpleTests(ITestOutputHelper output)
		{
			this._output = output;
		}

		#endregion
	}

	/// <summary>Message class used for testing</summary>
	public class Message
	{
		public int MessageNumber { get; set; }
	}

	/// <inheritdoc />
	public class MessageHandler : IHandleMessages<Message>
	{
		/// <inheritdoc />
		public Task Handle(Message evnt)
		{
			SimpleTests.Amount += evnt.MessageNumber;
			_output.WriteLine($"Received : \"{evnt.MessageNumber}\"");
			return Task.CompletedTask;
		}

		private readonly ITestOutputHelper _output;

		/// <summary>Creates new instance <see cref="MessageHandler"/>.</summary>
		public MessageHandler(ITestOutputHelper output)
		{
			_output = output;
		}
	}
}
