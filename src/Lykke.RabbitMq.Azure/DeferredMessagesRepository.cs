using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.RabbitMqBroker.Publisher.DeferredMessages;
using Lykke.SettingsReader;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.RabbitMq.Azure
{
    /// <summary>
    /// Stores deferred messages in the Azure Table Storage.
    /// Note, that message size is limited to the 65 Kb 
    /// and you can publish only up to the 1000 messages for the same delivery moment
    /// </summary>
    [PublicAPI]
    public sealed class DeferredMessagesRepository : IDeferredMessagesRepository
    {
        private const int MaxMessagesForTheSameDeliveryDate = 1000; // Should be power of 10
        private const int MaxMessagesForTheSameDeliveryDateDigits = 3; // Log(MaxMessagesForTheSameDeliveryDate)

        private static long _counter;
        private readonly INoSQLTableStorage<DeferredMessageEntity> _storage;
        private readonly DeferredMessagesPartitionSize _partitionSize;

        /// <summary>
        /// Creates <see cref="DeferredMessagesRepository"/> with default settings.
        /// <see cref="DeferredMessagesRepository"/> stores deferred messages in the Azure Table Storage.
        /// Note, that message size is limited to the 65 Kb 
        /// and you can publish only up to the 1000 messages for the same delivery moment
        /// </summary>
        /// <param name="log">The log</param>
        /// <param name="connectionString">The connection string reloading manager</param>
        /// <param name="tableName">The table name. Ensure, that only your app uses this table</param>
        /// <param name="partitionSize">
        /// <p>
        /// Messages partition size in time.
        /// Choose actual value accroding to your typical deferred messages delay period and the messages rate.
        /// Good value is when amount of partitions is balanced with amount of the messages in the single partition.
        /// </p>
        /// <p>
        /// If your publishes about 70 messages per minute and in the average the message is deferred about 1 hour, 
        /// <see cref="DeferredMessagesPartitionSize.Minute"/> is a good choice. Because a single partition will 
        /// contains about 70 messages, and there will be about 60 partitions of not overdue messages in the storage
        /// </p>
        /// Default value is <see cref="DeferredMessagesPartitionSize.Minute"/>
        /// </param>
        public static IDeferredMessagesRepository Create(
            ILog log,
            IReloadingManager<string> connectionString,
            string tableName, DeferredMessagesPartitionSize partitionSize = DeferredMessagesPartitionSize.Minute)
        {
            return new DeferredMessagesRepository(
                AzureTableStorage<DeferredMessageEntity>.Create(connectionString, tableName, log),
                partitionSize);
        }

        /// <summary>
        /// Stores deferred messages in the Azure Table Storage.
        /// Note, that message size is limited to the 65 Kb 
        /// and you can publish only up to the 1000 messages for the same delivery moment
        /// </summary>
        /// <param name="storage">The storage</param>
        /// <param name="partitionSize">
        /// <p>
        /// Messages partition size in time.
        /// Choose actual value accroding to your typical deferred messages delay period and the messages rate.
        /// Good value is when amount of partitions is balanced with amount of the messages in the single partition.
        /// </p>
        /// <p>
        /// If your publishes about 70 messages per minute and in the average the message is deferred about 1 hour, 
        /// <see cref="DeferredMessagesPartitionSize.Minute"/> is a good choice. Because a single partition will 
        /// contains about 70 messages, and there will be about 60 partitions of not overdue messages in the storage
        /// </p>
        /// Default value is <see cref="DeferredMessagesPartitionSize.Minute"/>
        /// </param>
        public DeferredMessagesRepository(
            INoSQLTableStorage<DeferredMessageEntity> storage, 
            DeferredMessagesPartitionSize partitionSize = DeferredMessagesPartitionSize.Minute)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _partitionSize = partitionSize;
        }

        /// <inheritdoc cref="IDeferredMessagesRepository.SaveAsync"/>
        public async Task SaveAsync(byte[] message, DateTime deliverAt)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var pk = GetPartitionKey(deliverAt);
            var rk = GetRowKey(deliverAt, Interlocked.Increment(ref _counter));

            await _storage.InsertAsync(new DeferredMessageEntity
            {
                PartitionKey = pk,
                RowKey = rk,
                Message = message
            });
        }

        /// <inheritdoc cref="IDeferredMessagesRepository.GetOverdueMessagesAsync"/>
        public async Task<IReadOnlyList<DeferredMessageEnvelope>> GetOverdueMessagesAsync(DateTime forTheMoment)
        {
            const int resultsLimit = 1000;

            var toPartitionKey = GetPartitionKey(forTheMoment);
            var partitionFilter = TableQuery.GenerateFilterCondition(
                nameof(DeferredMessageEntity.PartitionKey), 
                QueryComparisons.LessThanOrEqual,
                toPartitionKey);
            var toRowKey = GetRowKey(forTheMoment.AddTicks(1), 0);
            var rowFilter = TableQuery.GenerateFilterCondition(
                nameof(DeferredMessageEntity.RowKey),
                QueryComparisons.LessThan,
                toRowKey);
            var filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowFilter);

            var tableQuery = new TableQuery<DeferredMessageEntity>().Where(filter);
            var result = new List<DeferredMessageEnvelope>();

            await _storage.ExecuteAsync(tableQuery, messages =>
                {
                    var envelopes = messages.Select(message =>
                        new DeferredMessageEnvelope(
                            $"{message.PartitionKey}&{message.RowKey}",
                            message.Message));
                    result.AddRange(envelopes);
                },
                // Limits results count
                () => result.Count < resultsLimit);

            return result;
        }

        /// <inheritdoc cref="IDeferredMessagesRepository.RemoveAsync"/>
        public async Task RemoveAsync(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var parts = key.Split(new[] {'&'}, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length != 2)
            {
                throw new InvalidOperationException($"Invalid key [{key}] cannot be parsed");
            }

            var partitionKey = parts[0];
            var rowKey = parts[1];

            await _storage.DeleteAsync(partitionKey, rowKey);
        }

        private string GetPartitionKey(DateTime deliveryAt)
        {
            switch (_partitionSize)
            {
                case DeferredMessagesPartitionSize.Second:
                    return deliveryAt.ToString("yyyy-MM-ddTHH-mm-ss");

                case DeferredMessagesPartitionSize.Minute:
                    return deliveryAt.ToString("yyyy-MM-ddTHH-mm-00");

                case DeferredMessagesPartitionSize.Hour:
                    return deliveryAt.ToString("yyyy-MM-ddTHH-00-00");

                case DeferredMessagesPartitionSize.Day:
                    return deliveryAt.ToString("yyyy-MM-ddT00-00-00");

                case DeferredMessagesPartitionSize.Week:
                    return deliveryAt.RoundToWeek().ToString("yyyy-MM-ddT00-00-00");

                case DeferredMessagesPartitionSize.Month:
                    return deliveryAt.ToString("yyyy-MM-01T00-00-00");

                default:
                    throw new ArgumentOutOfRangeException(nameof(_partitionSize), _partitionSize, string.Empty);
            }
        }

        private string GetRowKey(DateTime deliveryAt, long counter)
        {
            var counterString = (counter % MaxMessagesForTheSameDeliveryDate).ToString($"D{MaxMessagesForTheSameDeliveryDateDigits}");

            return $"{deliveryAt:yyyy-MM-ddTHH-mm-ss.fffffff}.{counterString}";
        }
    }
}
