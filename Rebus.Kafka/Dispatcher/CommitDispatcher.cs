using Confluent.Kafka;
using Rebus.Kafka.Configs;
using Rebus.Kafka.Core;
using Rebus.Kafka.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Rebus.Kafka.Dispatcher
{

    /// <summary>
    /// Tracks which messages have already been processed and can be fixed and which need to be re-processed.
    /// </summary>
    internal class CommitDispatcher
    {
        internal ConcurrentDictionary<string, ProcessedMessage> _messageInfos = new ConcurrentDictionary<string, ProcessedMessage>();

        internal Result AppendMessage(TransportMessage message, TopicPartitionOffset topicPartitionOffset)
        {
            string messageId = message.GetId();
            if (_messageInfos.TryAdd(messageId, new ProcessedMessage(topicPartitionOffset, MessageProcessingStatuses.Processing)))
            {
#if DEBUG
                _log.Debug($"AppendMessage (Thread #{Thread.CurrentThread.ManagedThreadId}) message: {messageId}.{DicpatcherStateToStrting()}");
#endif
                return Result.Ok();
            }
            else
            {
                return Result.Fail($"Already exist {messageId} in {DicpatcherStateToStrting()}");
            }
        }

        internal Result Completing(TransportMessage message)
        {
            string messageId = message.GetId();
            if (_messageInfos.TryGetValue(messageId, out var oldProcessedMessage))
            {
                if (_messageInfos.TryUpdate(messageId, new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Completed), oldProcessedMessage))
                {
#if DEBUG
                    _log.Debug($"Completing message: {messageId}.{DicpatcherStateToStrting()}");
#endif
                    if (_messageInfos.Count >= _behaviorConfig.CommitPeriod && TryGetOffsetsThatCanBeCommit(out var tpos))
                    {
                        if (CanCommit(tpos).Failure)
                        {
                            var result = CanCommit(tpos);
                            if (result.Failure)
                            {
                                _log.Warn(result.Reason);
                                // ToDo: Return it back to try it next time. 
                            }
                        }
                    }
                    return Result.Ok();
                }
            }
            return Result.Fail($"No such message: {message.ToReadableText()} in the existing:\n{string.Join("\n", _messageInfos.Keys)}");
        }

        internal Result Reprocessing(TransportMessage message)
        {
            string messageId = message.GetId();
            if (_messageInfos.TryGetValue(messageId, out var oldProcessedMessage))
            {
                var newProcessedMessage = new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Reprocess, message);
                if (_messageInfos.TryUpdate(messageId, newProcessedMessage, oldProcessedMessage))
                {
#if DEBUG
                    _log.Debug($"Reprocessing message: {messageId}.{DicpatcherStateToStrting()}");
#endif                    
                    return Result.Ok();
                }
            }
            return Result.Fail($"No such message: {message.ToReadableText()}.{DicpatcherStateToStrting()}");
        }

        internal bool TryConsumeMessageToRestarted(out TransportMessage reprocessMessage)
        {
            lock (_tryConsumeMessageToRestartedLocker)
            {
                var reprocessMessageInfo = _messageInfos.OrderBy(mi => mi.Value.TopicPartitionOffset.Offset.Value)
                .FirstOrDefault(mi => mi.Value.Status == MessageProcessingStatuses.Reprocess);
                if (!reprocessMessageInfo.Equals(default(KeyValuePair<string, ProcessedMessage>)))
                {
                    _messageInfos.TryUpdate(reprocessMessageInfo.Key, new ProcessedMessage(reprocessMessageInfo.Value.TopicPartitionOffset, MessageProcessingStatuses.Processing), reprocessMessageInfo.Value);
                    {
                        reprocessMessage = reprocessMessageInfo.Value.Message;
#if DEBUG
                        _log.Debug($"TryConsumeMessageToRestarted message: {reprocessMessageInfo.Key}.{DicpatcherStateToStrting()}");
#endif
                        return true;
                    }
                }
                reprocessMessage = null;
                return false;
            }
        }

        internal bool TryGetOffsetsThatCanBeCommit(out List<TopicPartitionOffset> tpos)
        {
            tpos = new List<TopicPartitionOffset>();
            lock (_tryGetOffsetsThatCanBeCommitLocker)
            {
                var groups = _messageInfos.GroupBy(mi => new { mi.Value.TopicPartitionOffset.Topic, mi.Value.TopicPartitionOffset.Partition.Value });
                foreach (var group in groups)
                {
                    TopicPartitionOffset result = null;
                    foreach (var mi in group.OrderBy(pm => pm.Value.TopicPartitionOffset.Offset.Value))
                    {
                        if (mi.Value.Status == MessageProcessingStatuses.Completed)
                        {
                            _messageInfos.TryRemove(mi.Key, out _);
                            result = mi.Value.TopicPartitionOffset;
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (result != null)
                    {
                        tpos.Add(result);
                    }
                }
            }
            if (tpos.Count > 0)
            {
#if DEBUG
                _log.Debug($"TryCommitLastBlock offsets:\n\t{string.Join(",\n\t", tpos.Select(tpo => $"Topic:{tpo.Topic}, Partition:{tpo.Partition.Value}, Offset:{tpo.Offset.Value}"))}.{DicpatcherStateToStrting()}");
#endif
                return true;
            }
            else
            {
                //#if DEBUG
                //                _log.Debug($"CommitDispatcher.TryCommitLastBlock there is nothing to commit.{DicpatcherStateToStrting()}");
                //#endif
                return false;
            }
        }

        /// <summary>
        /// The event occurs when the most senior message block has been successfully processed
        /// </summary>
        /// <param name="commitAction"></param>
        internal void OnCanCommit(Func<IReadOnlyList<TopicPartitionOffset>, Result> commitAction)
        {
            _onCanCommit += commitAction;
        }
        event Func<IReadOnlyList<TopicPartitionOffset>, Result> _onCanCommit;

        Result CanCommit(IReadOnlyList<TopicPartitionOffset> topicPartitionOffset)
        {
            if (_onCanCommit == null) 
                return Result.Ok();

            var delegates = _onCanCommit.GetInvocationList();
            Result result;
            for (var index = 0; index < delegates.Length; index++)
            {
                // they're always of this type, so no need to check the type here
                var callback = (Func<IReadOnlyList<TopicPartitionOffset>, Result>)delegates[index];
                result = callback(topicPartitionOffset);
                if (result.Failure)
                {
                    return result;
                }
            }
            return Result.Ok();
        }

        readonly ILog _log;
        readonly ConsumerBehaviorConfig _behaviorConfig;
        object _tryGetOffsetsThatCanBeCommitLocker = new object();
        object _tryConsumeMessageToRestartedLocker = new object();

        internal CommitDispatcher(IRebusLoggerFactory rebusLoggerFactory, ConsumerBehaviorConfig behaviorConfig)
        {
            _log = rebusLoggerFactory.GetLogger<CommitDispatcher>();
            _behaviorConfig = behaviorConfig;
        }

        private string DicpatcherStateToStrting()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("\nMessage infos:");
            var latestMessageInfos = _messageInfos.Select(mi => $"{mi.Key}; {mi.Value}");
            sb.AppendLine($"\t{(latestMessageInfos.Any() ? string.Join("\n\t", latestMessageInfos) : "----")}");
            return sb.ToString();
        }
    }
}