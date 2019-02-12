using Autofac;
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
					if (message.MessageNumber == messageCount)
						_output.WriteLine($"Получено {messageCount} сообщений за {sw.ElapsedMilliseconds / 1000f:N3}с");
					return Task.CompletedTask;
				});

				Configure.With(adapter)
					.Logging(l => l.ColoredConsole(minLevel: LogLevel.Info))
					.Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), "temp"))
					.Routing(r => r.TypeBased().Map<Message>(nameof(SimpleTests)))
					.Start();

				var sendAmount = 0;
				var messages = Enumerable.Range(1, messageCount)
					.Select(i =>
					{
						sendAmount = sendAmount + i;
						return adapter.Bus.Send(new Message { MessageNumber = i});
					}).ToArray();

				Task.WaitAll(messages);
				Thread.Sleep(10000);

				Assert.Equal(amount, sendAmount);
			}
		}

		[Fact]
		public void PublishSubscribe()
		{
			IContainer _container;
			var builder = new ContainerBuilder();
			builder.RegisterInstance(_output).As<ITestOutputHelper>().SingleInstance();
			builder.RegisterType<MessageHandler>().As(typeof(IHandleMessages<>).MakeGenericType(typeof(Message)));
			builder.RegisterRebus((configurer, context) => configurer
				.Logging(l => l.ColoredConsole(LogLevel.Info))
				.Transport(t => t.UseKafka(kafkaEndpoint, nameof(SimpleTests), "temp"))
				.Options(o => o.SetMaxParallelism(5))
			);

			using (_container = builder.Build())
			using (IBus bus = _container.Resolve<IBus>())
			{
				bus.Subscribe<Message>().Wait();

				var sendAmount = 0;
				bus.Publish(new Message { MessageNumber = 0 }).Wait();
				var messages = Enumerable.Range(1, messageCount)
					.Select(i =>
				   {
					   sendAmount = sendAmount + i;
					   return bus.Publish(new Message { MessageNumber = i });
				   }).ToArray();

				Task.WaitAll(messages);
				Thread.Sleep(10000);

				Assert.Equal(amount, sendAmount);
			}
		}

		#region Настройка

		internal static int amount;
		const int messageCount = 10;
		private readonly ITestOutputHelper _output;
		static readonly string kafkaEndpoint = "192.168.0.166:9092";

		/// <summary>Создает новый экземпляр <see cref="SimpleTests"/>.</summary>
		public SimpleTests(ITestOutputHelper output)
		{
			this._output = output;
		}

		#endregion
	}

	/// <summary>Классс передаваемого сообщения</summary>
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
			SimpleTests.amount += evnt.MessageNumber;
			_output.WriteLine($"Received : \"{evnt.MessageNumber}\"");
			return Task.CompletedTask;
		}

		private readonly ITestOutputHelper _output;

		/// <summary>Создает новый экземпляр <see cref="MessageHandler"/>.</summary>
		public MessageHandler(ITestOutputHelper output)
		{
			_output = output;
		}
	}
}
