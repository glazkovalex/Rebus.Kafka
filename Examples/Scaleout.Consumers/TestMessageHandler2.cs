using Rebus.Bus;
using Rebus.Handlers;
using Scaleout.Messages;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Scaleout.Consumers
{
	/// <inheritdoc />
	public class TestMessageHandler2 : IHandleMessages<TestMessage2>
	{
		/// <inheritdoc />
		public async Task Handle(TestMessage2 evnt)
		{
			await Task.Delay(1000);
			Console.WriteLine($"Received Message2 : \"{evnt.MessageNumber}\" in thread {Thread.CurrentThread.ManagedThreadId}. Send Confirmation...");
			await _bus.Publish(new Confirmation { MessageNumber = evnt.MessageNumber });
		}

		public static int Amount;
		private readonly IBus _bus;
		public TestMessageHandler2(IBus bus)
		{
			_bus = bus;
		}
	}
}
