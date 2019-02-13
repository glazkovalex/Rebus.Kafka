using Rebus.Handlers;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BusScaleoutWitchDetailedConfiguration
{
	/// <inheritdoc />
	public class MessageHandler : IHandleMessages<Message>
	{
		/// <inheritdoc />
		public async Task Handle(Message evnt)
		{
			await Task.Delay(1000);
			Console.WriteLine($"Received : \"{evnt.MessageNumber}\" in thread {Thread.CurrentThread.ManagedThreadId}");
			_counter.Add(evnt.MessageNumber);
		}

		public static int Amount;
		private readonly Counter _counter;
		public MessageHandler(Counter counter)
		{
			_counter = counter;
		}
	}

	/// <summary>Message class used for testing</summary>
	public class Message
	{
		public int MessageNumber { get; set; }
	}

	public class Counter
	{
		public int Amount { get; private set; }
		public int Count { get; private set; }

		public int Add(int item)
		{
			lock (this)
			{
				Amount += item;
				if (++Count >= _expectedItemCount)
				{
					Console.WriteLine($"The sum of the {_expectedItemCount} values is {Amount}");
					Reset();
				}
				return Amount;
			}
		}

		public void Reset()
		{
			Amount = 0;
			Count = 0;
		}

		private readonly int _expectedItemCount;
		public Counter(int expectedItemCount)
		{
			_expectedItemCount = expectedItemCount;
		}
	}
}
