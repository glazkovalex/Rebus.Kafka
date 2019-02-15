using Rebus.Logging;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Core
{
	/// <summary>
	/// Logger factory that writes log statements using the <see cref="Trace"/> API
	/// </summary>
	public class TestOutputLoggerFactory : AbstractRebusLoggerFactory
	{
		/// <summary>
		/// Gets a <see cref="TestOutputLogger"/>
		/// </summary>
		protected override ILog GetLogger(Type type)
		{
			return new TestOutputLogger(type, this, _output, ShowTimestamps);
		}

		/// <summary>
		/// Gets or sets the minimum logging level to output
		/// </summary>
		public LogLevel MinLevel
		{
			get => _minLevel;
			set
			{
				_minLevel = value;
				Loggers.Clear();
			}
		}

		/// <summary>
		/// Gets/sets whether timestamps should be shown when logging
		/// </summary>
		public bool ShowTimestamps
		{
			get => _showTimestamps;
			set
			{
				_showTimestamps = value;
				Loggers.Clear();
			}
		}
		bool _showTimestamps;

		static readonly ConcurrentDictionary<Type, ILog> Loggers = new ConcurrentDictionary<Type, ILog>();
		LogLevel _minLevel = LogLevel.Debug;
		private readonly ITestOutputHelper _output;

		public TestOutputLoggerFactory(ITestOutputHelper output)
		{
			_output = output;
		}

		class TestOutputLogger : ILog
		{
			readonly Type _type;
			readonly TestOutputLoggerFactory _factory;
			private readonly ITestOutputHelper _output;
			readonly string _logLineFormatString;

			public TestOutputLogger(Type type, TestOutputLoggerFactory loggerFactory, ITestOutputHelper output, bool showTimestamps)
			{
				_type = type;
				_factory = loggerFactory;
				_output = output;
				_logLineFormatString = showTimestamps
					? "[{2}] {0} {1} ({3}): {4}"
					: "[{2}] {1} ({3}): {4}";
			}

			public void Debug(string message, params object[] objs)
			{
				Write(LogLevel.Debug, message, objs);
			}

			public void Info(string message, params object[] objs)
			{
				Write(LogLevel.Info, message, objs);
			}

			public void Warn(string message, params object[] objs)
			{
				Write(LogLevel.Warn, message, objs);
			}

			public void Warn(Exception exception, string message, params object[] objs)
			{
				Write(LogLevel.Warn, string.Concat(_factory.RenderString(message, objs), Environment.NewLine, exception));
			}

			public void Error(Exception exception, string message, params object[] objs)
			{
				Write(LogLevel.Error, string.Concat(_factory.RenderString(message, objs), Environment.NewLine, exception));
			}

			public void Error(string message, params object[] objs)
			{
				Write(LogLevel.Error, message, objs);
			}

			void Write(LogLevel level, string message, params object[] objs)
			{
				if ((int)level < (int)_factory.MinLevel) return;

				var levelString = LevelString(level);

				var threadName = GetThreadName();
				var typeName = _type.FullName;
				try
				{
					var renderedMessage = _factory.RenderString(message, objs);
					var timeFormat = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");

					// ReSharper disable EmptyGeneralCatchClause
					try
					{
						_output.WriteLine(_logLineFormatString,
							timeFormat,
							typeName,
							levelString,
							threadName,
							renderedMessage);
					}
					catch
					{
						// nothing to do about it if this part fails   
					}
					// ReSharper restore EmptyGeneralCatchClause
				}
				catch
				{
					Warn("Could not render {message} with args {args}", message, objs);
				}
			}

			static string GetThreadName()
			{
				var threadName = Thread.CurrentThread.Name;

				return string.IsNullOrWhiteSpace(threadName)
					? $"Thread #{Thread.CurrentThread.ManagedThreadId}"
					: threadName;
			}

			string LevelString(LogLevel level)
			{
				switch (level)
				{
					case LogLevel.Debug:
						return "DBG";
					case LogLevel.Info:
						return "INF";
					case LogLevel.Warn:
						return "WRN";
					case LogLevel.Error:
						return "ERR";
					default:
						throw new ArgumentOutOfRangeException(nameof(level));
				}
			}
		}
	}
}
