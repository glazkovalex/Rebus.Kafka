using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Rebus.Kafka
{
    public class KafkaAdmin
    {
        /// <summary>
        /// Get existing topic metadatas
        /// </summary>
        /// <param name="newTopicSpecifications">new topic specifications</param>
        /// <returns>existing topic metadatas</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="CreateTopicsException"></exception>
        public IReadOnlyList<TopicMetadata> GetExistingTopics(IEnumerable<TopicSpecification> newTopicSpecifications)
        {
            if (string.IsNullOrEmpty(_bootstrapServers))
                throw new ArgumentException("BootstrapServers it shouldn't be null!");

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build())
            {
                return adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                    .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                    .ToList();
            }
        }

        /// <summary>
        /// Try to create topics if there are none
        /// </summary>
        /// <param name="newTopicSpecifications">new topic specifications</param>
        /// <returns>existing topic metadatas</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="CreateTopicsException"></exception>
        public Task<IReadOnlyList<TopicMetadata>> CreateTopicsAsync(params TopicSpecification[] newTopicSpecifications)
            => CreateTopicsAsync(newTopicSpecifications as IEnumerable<TopicSpecification>);

        /// <summary>
        /// Try to create topics if there are none
        /// </summary>
        /// <param name="newTopicSpecifications">new topic specifications</param>
        /// <returns>existing topic metadatas</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="CreateTopicsException"></exception>
        public async Task<IReadOnlyList<TopicMetadata>> CreateTopicsAsync(IEnumerable<TopicSpecification> newTopicSpecifications)
        {
            if (string.IsNullOrEmpty(_bootstrapServers))
                throw new ArgumentException("BootstrapServers it shouldn't be null!");

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build())
            {
                var existingTopicMetadatas = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                    .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                    .ToList();
                var existingsTopics = existingTopicMetadatas.Select(t => t.Topic).ToList();
                var missingTopics = newTopicSpecifications.Where(t => !existingsTopics.Contains(t.Name)).ToList();
                if (missingTopics.Any())
                {
                    try
                    {
                        await adminClient.CreateTopicsAsync(missingTopics.Select(ts
                            => new TopicSpecification { Name = ts.Name, ReplicationFactor = ts.ReplicationFactor, NumPartitions = ts.NumPartitions }),
                            new CreateTopicsOptions { ValidateOnly = false }).ConfigureAwait(false);
                        existingTopicMetadatas = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                            .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                            .ToList();
                        existingsTopics = existingTopicMetadatas.Select(t => t.Topic).ToList();
                        var stillMissingTopics = newTopicSpecifications.Where(t => !existingsTopics.Contains(t.Name)).ToList();
                        if (stillMissingTopics.Any())
                        {
                            throw new ArgumentException($"Failed to create topics: \"{string.Join("\", \"", stillMissingTopics.Select(t => t.Name))}\"!", nameof(newTopicSpecifications));
                        }
                        else
                        {
                            _log?.LogInformation($"The topics were created: {string.Join(",", missingTopics)}!.");
                        }
                    }
                    catch (CreateTopicsException e)
                    {
                        _log?.LogError($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                        throw;
                    }
                }
                return existingTopicMetadatas;
            }
        }

        /// <summary>
        /// Try to delete topics if there are none
        /// </summary>
        /// <param name="deletedTopicSpecifications">deleted topic specifications</param>
        /// <param name="deleteTopicsOptions">delete topics options</param>
        /// <returns>existing topic metadatas</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="CreateTopicsException"></exception>
        public async Task<IReadOnlyList<TopicMetadata>> DeleteTopicsAsync(IEnumerable<string> deletedTopicSpecifications
            , DeleteTopicsOptions deleteTopicsOptions = default)
        {
            if (string.IsNullOrEmpty(_bootstrapServers))
                throw new ArgumentException("BootstrapServers it shouldn't be null!");

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build())
            {
                var existingTopicMetadatas = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                    .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                    .ToList();
                var existingsTopics = existingTopicMetadatas.Select(t => t.Topic).ToList();
                var deletedTopics = deletedTopicSpecifications.Where(t => existingsTopics.Contains(t)).ToList();
                if (deletedTopics.Any())
                {
                    try
                    {
                        await adminClient.DeleteTopicsAsync(deletedTopics, deleteTopicsOptions).ConfigureAwait(false);
                        existingTopicMetadatas = adminClient.GetMetadata(TimeSpan.FromSeconds(100)).Topics
                            .Where(topicMetadata => topicMetadata.Error.Code != ErrorCode.UnknownTopicOrPart || topicMetadata.Error.Code == ErrorCode.Local_UnknownTopic)
                            .ToList();
                        existingsTopics = existingTopicMetadatas.Select(t => t.Topic).ToList();
                        var stillMissingTopics = deletedTopicSpecifications.Where(t => existingsTopics.Contains(t)).ToList();
                        if (stillMissingTopics.Any())
                        {
                            throw new ArgumentException($"Failed to delete topics: \"{string.Join("\", \"", stillMissingTopics)}\"!", nameof(deletedTopicSpecifications));
                        }
                        else
                        {
                            _log?.LogWarning($"The topics were deleted: {string.Join(",", deletedTopics)}!.");
                        }
                    }
                    catch (CreateTopicsException e)
                    {
                        _log?.LogError($"An error occured deleting topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                        throw;
                    }
                }
                return existingTopicMetadatas;
            }
        }

        private string _bootstrapServers;
        ILogger _log;

        public KafkaAdmin(string bootstrapServers, ILogger log = null)
        {
            if (string.IsNullOrEmpty(bootstrapServers))
                throw new ArgumentException("BootstrapServers it shouldn't be null!");
            _bootstrapServers = bootstrapServers;
            _log = log;
        }
    }
}
