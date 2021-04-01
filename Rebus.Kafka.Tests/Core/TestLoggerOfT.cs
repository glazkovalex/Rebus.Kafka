using System;
using Microsoft.Extensions.Logging;
using Rebus.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Rebus.Kafka.Tests.Core
{
    public class TestLogger<T> : ILogger<T>
    {
        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            throw new NotImplementedException();
        }
        
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            switch(logLevel)
            {
                case LogLevel.Error:
                    _logger.Error(exception, formatter.Invoke(state, exception)); break;
                case LogLevel.Warning:
                    _logger.Warn(formatter.Invoke(state, exception)); break;
                case LogLevel.Information: 
                    _logger.Info(formatter.Invoke(state, exception)); break;
                default:
                    _logger.Debug(formatter.Invoke(state, exception)); break;
            }
        }

        private readonly ILog _logger;
        public TestLogger(ILog logger)
        {
            _logger = logger;
        }
    }

}
