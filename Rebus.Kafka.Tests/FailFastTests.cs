using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Kafka.Tests.Core;
using Rebus.Kafka.Tests.ErrorHandling;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Retry;
using Rebus.Retry.FailFast;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Transport.InMem;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests
{
    public class FailFastTests : BaseTestWithKafkaContainer
    {
        static readonly string InputQueueName = TestConfig.GetName($"test.rebus2.retries.input@{GetMachineName()}");
        static readonly string ErrorQueueName = TestConfig.GetName("rebus2.error");

        BuiltinHandlerActivator _handlerActivator;
        IBus _bus;
        InMemNetwork _network;
        CustomErrorHandler _customErrorHandler;

        static string GetMachineName() => Environment.MachineName;

        void InitializeBus(int numberOfRetries, IFailFastChecker failFastChecker = null, CustomErrorHandler customErrorHandler = null)
        {
            _network = new InMemNetwork();

            _handlerActivator = new BuiltinHandlerActivator();

            _bus = Configure.With(_handlerActivator)
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseInMemoryTransport(_network, InputQueueName))
                .Routing(r => r.TypeBased().Map<string>(InputQueueName))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);

                    o.SimpleRetryStrategy(maxDeliveryAttempts: numberOfRetries, errorQueueAddress: ErrorQueueName);

                    if (failFastChecker != null)
                    {
                        o.Register(_ => failFastChecker);
                    }
                    if (customErrorHandler != null)
                    {
                        o.Register<IErrorHandler>(c => _customErrorHandler);
                    }
                })
                .Start();
        }

        [Fact]
        public async Task ItWorks()
        {
            const int numberOfRetries = 5;

            InitializeBus(numberOfRetries);

            var attemptedDeliveries = 0;

            WithoutAnyWorkers(_handlerActivator.Bus, () => _handlerActivator.Handle<string>(async _ =>
            {
                Interlocked.Increment(ref attemptedDeliveries);
                throw new FailFastException("omgwtf!");
            }));

            await _bus.Send("hej");

            var failedMessage = await WaitForNextMessageFrom(_network, ErrorQueueName);

            Assert.Equal(1, attemptedDeliveries);
            Assert.Contains("1 unhandled exceptions", failedMessage.Headers.GetValue(Headers.ErrorDetails));
            Assert.Equal(failedMessage.Headers.GetValue(Headers.SourceQueue), InputQueueName);
        }

        [Fact]
        public async Task ItUsesSimpleRetryStrategyWhenCustomException()
        {
            const int numberOfRetries = 5;

            InitializeBus(numberOfRetries);

            var attemptedDeliveries = 0;

            WithoutAnyWorkers(_handlerActivator.Bus, () => _handlerActivator.Handle<string>(async _ =>
            {
                Interlocked.Increment(ref attemptedDeliveries);
                throw new InvalidOperationException("omgwtf!");
            }));

            await _bus.Send("hej");

            var failedMessage = await WaitForNextMessageFrom(_network, ErrorQueueName);

            Assert.Equal(numberOfRetries, attemptedDeliveries);
            Assert.Contains($"{numberOfRetries} unhandled exceptions", failedMessage.Headers.GetValue(Headers.ErrorDetails));
            Assert.Equal(failedMessage.Headers.GetValue(Headers.SourceQueue), InputQueueName);
        }

        [Fact]
        public async Task CanConfigureCustomFailFastChecker()
        {
            const int numberOfRetries = 5;

            InitializeBus(numberOfRetries, new CustomFailFastChecker());

            var attemptedDeliveries = 0;

            WithoutAnyWorkers(_handlerActivator.Bus, () => _handlerActivator.Handle<string>(async _ =>
            {
                Interlocked.Increment(ref attemptedDeliveries);
                throw new InvalidOperationException("omgwtf!");
            }));

            await _bus.Send("hej");

            var failedMessage = await WaitForNextMessageFrom(_network, ErrorQueueName);

            Assert.Equal(1, attemptedDeliveries);
            Assert.Contains("1 unhandled exceptions", failedMessage.Headers.GetValue(Headers.ErrorDetails));
            Assert.Equal(failedMessage.Headers.GetValue(Headers.SourceQueue), InputQueueName);
        }

        [Fact]
        public async Task ForwardsFailedMessageToCustomErrorHandler()
        {
            _customErrorHandler = new CustomErrorHandler();
            const int numberOfRetries = 5;
            InitializeBus(numberOfRetries, customErrorHandler: _customErrorHandler);

            WithoutAnyWorkers(_handlerActivator.Bus, () => _handlerActivator.Handle<string>(async str =>
            {
                Output.WriteLine("Throwing UnauthorizedAccessException");

                throw new UnauthorizedAccessException("don't do that");
            }));

            _handlerActivator.Bus.SendLocal("hej 2").Wait();

            await WaitUntil(_customErrorHandler.FailedMessages, q => q.Any());

            var transportMessage = _customErrorHandler.FailedMessages.First();

            Assert.Equal(@"""hej 2""", Encoding.UTF8.GetString(transportMessage.Body));
        }

        public static async Task WaitUntil<T>(ConcurrentQueue<T> queue, Expression<Func<ConcurrentQueue<T>, bool>> criteriaExpression, int? timeoutSeconds = 5)
        {
            var start = DateTime.UtcNow;
            var timeout = TimeSpan.FromSeconds(timeoutSeconds.GetValueOrDefault(5));
            var criteria = criteriaExpression.Compile();

            while (true)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100));

                if (criteria(queue)) break;

                if (DateTime.UtcNow - start < timeout) continue;

                throw new TimeoutException($"Criteria {criteriaExpression} not satisfied within {timeoutSeconds} s timeout");
            }
        }

        static void WithoutAnyWorkers(IBus bus, Action action)
        {
            var api = bus.Advanced.Workers;
            var count = api.Count;
            api.SetNumberOfWorkers(0);
            action();
            api.SetNumberOfWorkers(count);
        }

        public static async Task<TransportMessage> WaitForNextMessageFrom(InMemNetwork network, string queueName, int timeoutSeconds = 5)
        {
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                var nextMessage = network.GetNextOrNull(queueName);

                if (nextMessage != null)
                {
                    return nextMessage.ToTransportMessage();
                }

                await Task.Delay(100);

                if (stopwatch.Elapsed < TimeSpan.FromSeconds(timeoutSeconds))
                    continue;

                throw new TimeoutException($"Did not receive message from queue '{queueName}' within {timeoutSeconds} s timeout");
            }
        }

        public FailFastTests(ITestOutputHelper output) : base(output) { }
    }
}
