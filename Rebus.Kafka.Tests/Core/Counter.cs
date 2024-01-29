using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Rebus.Kafka.Tests.Core
{
	public class Counter
	{
		public int Amount => Items.Sum(m => m.MessageNumber);
		public int Count => Items.Count;
		public ConcurrentBag<dynamic> Items { get; private set; } = new ConcurrentBag<dynamic>();

		public void Add(dynamic item)
		{
			lock (this)
			{
				Items.Add(item);
				if (Items.Count >= _expectedItemCount)
				{
					Console.WriteLine($"The sum of the {_expectedItemCount} values is {Amount}");
					Reset();
				}
			}
		}

		public void Reset()
		{
			Items.Clear();
		}

		private readonly int _expectedItemCount;
		public Counter(int expectedItemCount = Int32.MaxValue)
		{
            _expectedItemCount = expectedItemCount;
		}
	}
}
