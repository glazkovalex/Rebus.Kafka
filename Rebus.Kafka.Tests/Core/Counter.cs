using System;

namespace Rebus.Kafka.Tests.Core
{
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
		public Counter(int expectedItemCount = Int32.MaxValue)
		{
			_expectedItemCount = expectedItemCount;
		}
	}
}
