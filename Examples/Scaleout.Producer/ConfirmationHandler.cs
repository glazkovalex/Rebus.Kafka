using Rebus.Handlers;
using Scaleout.Messages;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Scaleout.Producer
{
	/// <inheritdoc />
	class ConfirmationHandler : IHandleMessages<Confirmation>
	{
		/// <inheritdoc />
		public async Task Handle(Confirmation evnt)
		{
			await Task.Delay(1000);
			Console.WriteLine($"Received confirmation for : \"{evnt.MessageNumber}\" in thread {Thread.CurrentThread.ManagedThreadId}");
			_counter.Add(evnt.MessageNumber);
		}

		public int Amount;
		private readonly Counter _counter;
		public ConfirmationHandler(Counter counter)
		{
			_counter = counter;
		}
	}
}
