using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.Common.Log;
using Lykke.RabbitMqBroker.Deduplication;
using Lykke.SettingsReader.ReloadingManager;

namespace Lykke.RabbitMq.Azure.Deduplicator
{
    /// <inheritdoc />
    /// <remarks>Azure table storage implementation</remarks>
    public class AzureStorageDeduplicator : IDeduplicator
    {
        private readonly DuplicatesRepository _repository;

        public AzureStorageDeduplicator(INoSQLTableStorage<DuplicateEntity> tableStorage)
        {
            _repository = new DuplicatesRepository(tableStorage);
        }

        public static AzureStorageDeduplicator Create(string connString, string tableName, ILogFactory log)
        {
            return new AzureStorageDeduplicator(AzureTableStorage<DuplicateEntity>.Create(
                ConstantReloadingManager.From(connString), tableName, log));
        }
        
        public async Task<bool> EnsureNotDuplicateAsync(byte[] value)
        {
            return await _repository.EnsureNotDuplicateAsync(value);
        }
    }
}
