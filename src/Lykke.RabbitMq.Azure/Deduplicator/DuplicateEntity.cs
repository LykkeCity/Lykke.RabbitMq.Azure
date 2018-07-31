using Lykke.RabbitMqBroker.Deduplication;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.RabbitMq.Azure.Deduplicator
{
    public class DuplicateEntity : TableEntity
    {
        private static string GeneratePartitionKey(string val) => $"{val}_PK";
        private static string GenerateRowKey(string val) => val;

        internal static DuplicateEntity Create(byte[] value)
        {
            var hash = HashHelper.GetMd5Hash(value);
            return new DuplicateEntity
            {
                PartitionKey = GeneratePartitionKey(hash),
                RowKey = GenerateRowKey(hash)
            };
        }
    }
}
