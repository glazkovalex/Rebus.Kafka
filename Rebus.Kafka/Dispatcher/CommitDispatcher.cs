using Confluent.Kafka;
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
        internal ConcurrentQueue<MessageBlock> _queue = new ConcurrentQueue<MessageBlock>();
        internal MessageBlock _latestMessageInfos = new MessageBlock();

        internal Result AppendMessage(TransportMessage message, TopicPartitionOffset topicPartitionOffset)
        {
            string messageId = message.GetId();
            if (_latestMessageInfos.TryAdd(messageId, new ProcessedMessage(topicPartitionOffset, MessageProcessingStatuses.Processing)))
            {
                _log.Debug($"\nCommitDispatcher.AppendMessage message: {messageId}.{DicpatcherStateToStrting()}");
                return Result.Ok();
            }
            else
            {
                return Result.Fail($"Already exist {messageId} in {DicpatcherStateToStrting()}");
            }
        }

        internal Result AppendMessageInQueue(TransportMessage message, TopicPartitionOffset topicPartitionOffset)
        {
            string messageId = message.GetId();
            if (_latestMessageInfos.TryAdd(messageId, new ProcessedMessage(topicPartitionOffset, MessageProcessingStatuses.Processing)))
            {
                var messageInfos = Interlocked.Exchange(ref _latestMessageInfos, new MessageBlock());
                _queue.Enqueue(messageInfos);
                _log.Debug($"\nCommitDispatcher.AppendMessageInQueue message: {messageId}.{DicpatcherStateToStrting()}");
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
            if (_latestMessageInfos.TryGetValue(messageId, out var oldProcessedMessage))
            {
                if (_latestMessageInfos.TryUpdate(messageId, new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Completed), oldProcessedMessage))
                {
                    _log.Debug($"\nCommitDispatcher.Completing message: {messageId}.{DicpatcherStateToStrting()}");
                    return Result.Ok();
                }
            }
            else
            {
                foreach (var block in _queue)
                {
                    if (block.TryGetValue(messageId, out oldProcessedMessage))
                    {
                        if (block.TryUpdate(messageId, new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Completed), oldProcessedMessage))
                        {
                            _log.Debug($"\nCommitDispatcher.Completing message: {messageId}.{DicpatcherStateToStrting()}");
                            while (TryCommitLastBlock()) { }
                            return Result.Ok();
                        }
                    }
                }
            }
            return Result.Fail($"No such message: {message.ToReadableText()} in the existing:\n{string.Join("\n", _latestMessageInfos.Keys.Concat(_queue.SelectMany(b => b.Keys)))}");
        }

        private bool TryCommitLastBlock()
        {
            if (_queue.TryPeek(out var block))
            {
                if (block.All(m => m.Value.Status == MessageProcessingStatuses.Completed))
                {
                    if (_queue.TryDequeue(out var completedBlock))
                    {
                        var tpos = completedBlock.Values.Select(mi => mi.TopicPartitionOffset).ToList();
                        _log.Debug($"\nCommitDispatcher.OnCanCommit messages:\n\t{string.Join(",\n\t", completedBlock.Select(mi => $"{mi.Key}; {mi.Value}"))}.{DicpatcherStateToStrting()}");
                        CanCommit(tpos);
                        return true;
                    }
                }
            }
            return false;
        }

        internal Result Reprocessing(TransportMessage message)
        {
            string messageId = message.GetId();
            if (_latestMessageInfos.TryGetValue(messageId, out var oldProcessedMessage))
            {
                var newProcessedMessage = new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Reprocess, message);
                if (_latestMessageInfos.TryUpdate(messageId, newProcessedMessage, oldProcessedMessage))
                {
                    _log.Debug($"\nCommitDispatcher.Reprocessing message: {messageId}.{DicpatcherStateToStrting()}");
                    return Result.Ok();
                }
            }
            else
            {
                foreach (var block in _queue)
                {
                    if (block.TryGetValue(messageId, out oldProcessedMessage))
                    {
                        var newProcessedMessage = new ProcessedMessage(oldProcessedMessage.TopicPartitionOffset, MessageProcessingStatuses.Reprocess, message);
                        if (block.TryUpdate(messageId, newProcessedMessage, oldProcessedMessage))
                        {
                            _log.Debug($"\nCommitDispatcher.Reprocessing message: {messageId}.{DicpatcherStateToStrting()}");
                            return Result.Ok();
                        }
                    }
                }
            }
            return Result.Fail($"No such message: {message.ToReadableText()}.{DicpatcherStateToStrting()}");
        }

        internal bool TryConsumeMessageToRestarted(out TransportMessage reprocessMessage)
        {
            if (_queue.TryPeek(out var block))
            {
                var pair = block.FirstOrDefault(m => m.Value.Status == MessageProcessingStatuses.Reprocess);
                if (!pair.Equals(default(KeyValuePair<string, ProcessedMessage>)))
                {
                    if (block.TryUpdate(pair.Key, new ProcessedMessage(pair.Value.TopicPartitionOffset, MessageProcessingStatuses.Processing ), pair.Value))
                    {
                        reprocessMessage = pair.Value.Message;
                        _log.Debug($"\nCommitDispatcher.TryConsumeMessageToRestarted message: {pair.Key}.{DicpatcherStateToStrting()}");
                        return true;
                    }
                }
            }
            //ToDo: Отслеживать задержки перезапуска
            reprocessMessage = null;
            return false;
        }

        /// <summary>
        /// The event occurs when the most senior message block has been successfully processed
        /// </summary>
        /// <param name="commitAction"></param>
        internal void OnCanCommit(Action<IReadOnlyList<TopicPartitionOffset>> commitAction)
        {
            _onCanCommit += commitAction;
        }
        event Action<IReadOnlyList<TopicPartitionOffset>> _onCanCommit;

        void CanCommit(IReadOnlyList<TopicPartitionOffset> topicPartitionOffset)
        {
            if (_onCanCommit == null) return;

            var delegates = _onCanCommit.GetInvocationList();

            for (var index = 0; index < delegates.Length; index++)
            {
                // they're always of this type, so no need to check the type here
                var callback = (Action<IReadOnlyList<TopicPartitionOffset>>)delegates[index];

                callback(topicPartitionOffset);
            }
        }

        readonly ILog _log;

        internal CommitDispatcher(ILog log)
        {
            _log = log;
        }

        private string DicpatcherStateToStrting()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine();
            int i = 0;
            foreach (var block in _queue)
            {
                sb.AppendLine($"Queue block# {(i == 0 ? $"{i} (oldest)" : i.ToString())} message infos:");
                sb.AppendLine($"\t{string.Join("\n\t", block.Select(mi => $"{mi.Key}; {mi.Value}"))}");
                i++;
            }
            sb.AppendLine("Latest message infos:");
            var latestMessageInfos = _latestMessageInfos.Select(mi => $"{mi.Key}; {mi.Value}");
            sb.AppendLine($"\t{(latestMessageInfos.Any() ? string.Join("\n\t", latestMessageInfos) : "----")}");
            return sb.ToString();
        }
    }
}