using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Kafka.Tests.Core;
using Rebus.Logging;
using Rebus.Persistence.InMem;
using Rebus.Retry.Simple;
using Rebus.Sagas;
using Rebus.Sagas.Idempotent;
using Rebus.Kafka.Tests.Extensions;
using Xunit;
using Xunit.Abstractions;
using Rebus.Kafka.Tests.Base;
using System.Collections.Generic;
using Rebus.Messages;

namespace Rebus.Kafka.Tests;

/// <summary>
/// Based on https://github.com/rebus-org/Rebus/blob/master/Rebus.Tests/Bugs/ReproduceFunnySagaIssue.cs"
/// </summary>
[Collection("Serial")]
public class SagaIssueTests : BaseTestWithKafkaContainer
{
    [Fact]
    public async Task SeeIfThisWorks()
    {
        var completedSagaInstanceIds = new ConcurrentQueue<string>();
        var listLoggerFactory = new TestOutputLoggerFactory(Output);

        using (var activator = new BuiltinHandlerActivator())
        {
            activator.Register((bús, _) => new TestSaga(bús, listLoggerFactory, completedSagaInstanceIds));

            var bus = Configure.With(activator)
                .Logging(l => l.Use(new TestOutputLoggerFactory(Output)))
                .Transport(t => t.UseKafka(BootstrapServer, $"{nameof(SeeIfThisWorks)}.queue", nameof(SagaIssueTests)))
                .Sagas(s => s.StoreInMemory()) // Never do that in production!
                .Start();

            await bus.Subscribe<SagaMessageEarth>();
            await bus.Subscribe<SagaMessageWind>();
            await bus.Subscribe<SagaMessageFire>();

            var starters = Enumerable.Range(0, 100)
                .Select(n => new KickoffSagaMessages { SagaInstanceId = n.ToString() })
                .ToList();

            await Task.WhenAll(starters.Select(starter => bus.SendLocal(starter)));

            await completedSagaInstanceIds.WaitUntil(q => q.Count == starters.Count, timeoutSeconds: 20);

            Assert.Empty(completedSagaInstanceIds.Except(starters.Select(s => s.SagaInstanceId)));
        }
    }

    public class KickoffSagaMessages
    {
        public string SagaInstanceId { get; set; }
    }

    public interface ISagaMessage
    {
        string SagaInstanceId { get; set; }
    }

    public class SagaMessageEarth : ISagaMessage
    {
        public string SagaInstanceId { get; set; }
    }

    public class SagaMessageWind : ISagaMessage
    {
        public string SagaInstanceId { get; set; }
    }

    public class SagaMessageFire : ISagaMessage
    {
        public string SagaInstanceId { get; set; }
    }

    public class TestSagaData : IdempotentSagaData
    {
        public string stuffDone;
        public bool Task1Processed { get; set; }
        public bool Task2Processed { get; set; }
        public bool Task3Processed { get; set; }
        public string SagaInstanceId { get; set; }
    }

    public class TestSaga : IdempotentSaga<TestSagaData>,
        IAmInitiatedBy<KickoffSagaMessages>,
        IHandleMessages<SagaMessageEarth>,
        IHandleMessages<SagaMessageWind>,
        IHandleMessages<SagaMessageFire>,
        IHandleMessages<IFailed<ISagaMessage>>
    {
        readonly ConcurrentQueue<string> _completedSagaInstanceIds;
        readonly ILog _logger;
        readonly IBus _bus;
        static bool first = true;

        public TestSaga(IBus bus, IRebusLoggerFactory rebusLoggerFactory, ConcurrentQueue<string> completedSagaInstanceIds)
        {
            _bus = bus ?? throw new ArgumentNullException();
            _completedSagaInstanceIds = completedSagaInstanceIds;
            _logger = rebusLoggerFactory.GetLogger<TestSaga>();
        }

        protected override async Task ResolveConflict(TestSagaData otherSagaData)
        {
            Data.Task1Processed = Data.Task1Processed || otherSagaData.Task1Processed;
            Data.Task2Processed = Data.Task2Processed || otherSagaData.Task2Processed;
            Data.Task3Processed = Data.Task3Processed || otherSagaData.Task3Processed;
        }

        public async Task Handle(KickoffSagaMessages message)
        {
            _logger.Info("Processing Kickoff - {id}", Data.SagaInstanceId);
            Data.SagaInstanceId = message.SagaInstanceId;
            Data.stuffDone += "Initiated;";
            string messageId = Guid.NewGuid().ToString();
            await Task.WhenAll(Enumerable.Range(1, 2).Select(async i =>
            {
                await _bus.Publish(new SagaMessageEarth
                {
                    SagaInstanceId = message.SagaInstanceId
                }, new Dictionary<string, string> { { Headers.MessageId, messageId } });
                _logger.Info("Published Earth....Done Processing Kickoff - {id}", Data.SagaInstanceId);
            }));
        }

        public async Task Handle(SagaMessageEarth message)
        {
            try
            {
                if (!Data.Task1Processed)
                {
                    _logger.Info("Processing Earth - {id}", Data.SagaInstanceId);
                    Data.stuffDone += "Earth;";
                    Data.Task1Processed = true;
                }
                await _bus.Publish(new SagaMessageWind()
                {
                    SagaInstanceId = message.SagaInstanceId
                });
                PossiblyDone();
                _logger.Info("Published Wind...Done Processing Earth - {id}", Data.SagaInstanceId);
            }
            catch (Exception e)
            {
                _logger.Error(e, "WHAT Earth? - {id}", Data.SagaInstanceId);
                throw;
            }
        }

        public async Task Handle(SagaMessageWind message)
        {
            try
            {
                if (!Data.Task2Processed)
                {
                    _logger.Info("Processing Wind - {id}", Data.SagaInstanceId);
                    Data.stuffDone += "Wind;";
                    Data.Task2Processed = true;
                }
                await _bus.Publish(new SagaMessageFire()
                {
                    SagaInstanceId = message.SagaInstanceId
                });
                PossiblyDone();
                if (first)
                {
                    first = false;
                    throw new InvalidOperationException("Text exeption");
                }
                _logger.Info("Published Fire...Done Processing Wind - {id}", Data.SagaInstanceId);
            }
            catch (Exception e)
            {
                _logger.Error(e, "WHAT Wind? - {id}", Data.SagaInstanceId);
                throw;
            }
        }

        public async Task Handle(SagaMessageFire message)
        {
            try
            {
                if (!Data.Task3Processed)
                {
                    //throw new Exception("Not going to finish this");
                    _logger.Info("Processing Fire - {id}", Data.SagaInstanceId);
                    Data.stuffDone += "Fire;";
                    Data.Task3Processed = true;
                }
                PossiblyDone();
                _logger.Info("Done Processing Fire - {id}", Data.SagaInstanceId);
            }
            catch (Exception e)
            {
                _logger.Error(e, "WHAT Fire? - {id}", Data.SagaInstanceId);
                throw;
            }
        }

        void PossiblyDone()
        {
            if (Data.Task1Processed && Data.Task2Processed && Data.Task3Processed)
            {
                _logger.Info("Completed everything for {id}: {msg}", Data.SagaInstanceId, Data.stuffDone);
                _completedSagaInstanceIds.Enqueue(Data.SagaInstanceId);
                MarkAsComplete();
            }
            else
            {
                _logger.Info("NOT Completed everything for {id}: {task1},{task2},{task3}", Data.SagaInstanceId, Data.Task1Processed, Data.Task2Processed, Data.Task3Processed);
            }
        }

        protected override void CorrelateMessages(ICorrelationConfig<TestSagaData> config)
        {
            config.Correlate<KickoffSagaMessages>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageFire>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageEarth>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageWind>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<IFailed<ISagaMessage>>(m => m.Message.SagaInstanceId, d => d.SagaInstanceId);
        }

        public async Task Handle(IFailed<ISagaMessage> message)
        {
            _logger.Error("Unable to handle the message of type {msgtype} with error message {errMsg}", message.Message.GetType().Name, message.ErrorDescription);
        }
    }

    public SagaIssueTests(ITestOutputHelper output) : base(output)
    {
    }
}