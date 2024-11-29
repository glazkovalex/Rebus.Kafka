using Rebus.Bus;
using Rebus.Handlers;
using Rebus.Retry.Simple;
using Rebus.Sagas.Idempotent;
using Rebus.Sagas;
using IdempotentSaga.Messages;
using Rebus.Logging;

namespace IdempotentSaga.Handlers
{
    public class TestSaga : IdempotentSaga<TestSagaData>,
                            IAmInitiatedBy<KickoffSagaMessages>,
                            IHandleMessages<SagaMessageEarth>,
                            IHandleMessages<SagaMessageWind>,
                            IHandleMessages<SagaMessageFire>,
                            IHandleMessages<KickoffSagaMessages>,
                            IHandleMessages<IFailed<ISagaMessage>>
    {
        public async Task Handle(KickoffSagaMessages message)
        {
            _logger.Info("Processing Kickoff - {id}", Data.SagaInstanceId);
            Data.SagaInstanceId = message.SagaInstanceId;
            Data.StuffDone += "Initiated;";
/*/
            string messageId = Guid.NewGuid().ToString();
            await Task.WhenAll(Enumerable.Range(1, 2).Select(async i =>
            {
                await _bus.Publish(new SagaMessageEarth
                {
                    SagaInstanceId = message.SagaInstanceId
                }, new Dictionary<string, string> { { Headers.MessageId, messageId } });
                _logger.Info("Published Earth....Done Processing Kickoff - {id}", Data.SagaInstanceId);
            }));
/*/
            await _bus.Publish(new SagaMessageEarth()
            {
                SagaInstanceId = message.SagaInstanceId
            });
            _logger.Info("Published Earth....Done Processing Kickoff - {id}", Data.SagaInstanceId);
/**/
        }

        public async Task Handle(SagaMessageEarth message)
        {
            try
            {
                if (!Data.Task1Processed)
                {
                    _logger.Info("Processing Earth - {id}", Data.SagaInstanceId);
                    Data.StuffDone += "Earth;";
                    Data.Task1Processed = true;
                }
                await _bus.Publish(new SagaMessageWind()
                {
                    SagaInstanceId = message.SagaInstanceId
                });
                PossiblyDone();
//#if DEBUG
//                if (first)
//                {
//                    first = false;
//                    throw new InvalidOperationException("Text exeption");
//                }
//#endif
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
                    Data.StuffDone += "Wind;";
                    Data.Task2Processed = true;
                }
                await _bus.Publish(new SagaMessageFire()
                {
                    SagaInstanceId = message.SagaInstanceId
                });
                PossiblyDone();

                _logger.Info("Published Fire...Done Processing Wind - {id}", Data.SagaInstanceId);
            }
            catch (Exception e)
            {
                _logger.Error(e, "WHAT Wind? - {id}", Data.SagaInstanceId);
                throw;
            }
        }

        public Task Handle(SagaMessageFire message)
        {
            try
            {
                if (!Data.Task3Processed)
                {
                    //throw new Exception("Not going to finish this");
                    _logger.Info("Processing Fire - {id}", Data.SagaInstanceId);
                    Data.StuffDone += "Fire;";
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
            return Task.CompletedTask;
        }

        private void PossiblyDone()
        {
            if (Data.Task1Processed && Data.Task2Processed && Data.Task3Processed)
            {
                _logger.Info("Completed everything for {id}: {msg}", Data.SagaInstanceId, Data.StuffDone);
                MarkAsComplete();
            }
            else
            {
                _logger.Info("NOT Completed everything for {id}: {task1},{task2},{task3}", Data.SagaInstanceId, Data.Task1Processed, Data.Task2Processed, Data.Task3Processed);
            }
        }

        public Task Handle(IFailed<ISagaMessage> message)
        {
            _logger.Error("Unable to handle the message of type {msgtype} with error message {errMsg}", message.Message.GetType().Name, message.ErrorDescription);
            return Task.CompletedTask;
        }

        protected override void CorrelateMessages(ICorrelationConfig<TestSagaData> config)
        {
            //config.CorrelateHeader<ISagaMessage>("rbs2-corr-id", d => d.Id);
            config.Correlate<KickoffSagaMessages>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageFire>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageEarth>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<SagaMessageWind>(m => m.SagaInstanceId, d => d.SagaInstanceId);
            config.Correlate<IFailed<ISagaMessage>>(m => m.Message.SagaInstanceId, d => d.SagaInstanceId);
        }

        protected override Task ResolveConflict(TestSagaData otherSagaData)
        {
            Data.Task1Processed = Data.Task1Processed || otherSagaData.Task1Processed;
            Data.Task2Processed = Data.Task2Processed || otherSagaData.Task2Processed;
            Data.Task3Processed = Data.Task3Processed || otherSagaData.Task3Processed;
            return Task.CompletedTask;
        }

        private IBus _bus;
        private ILog _logger;
#if DEBUG
        static bool first = true;
#endif
        public TestSaga(IBus bus, ConsoleLoggerFactory loggerFactory)
        {
            _bus = bus ?? throw new ArgumentNullException();
            _logger = loggerFactory.GetLogger<TestSaga>();
        }
    }
}
