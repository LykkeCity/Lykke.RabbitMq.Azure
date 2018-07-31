using System.Threading.Tasks;
using AzureStorage.Tables;
using Lykke.RabbitMq.Azure.Deduplicator;
using NUnit.Framework;

namespace Lykke.RabbitMq.Azure.Tests
{
    [TestFixture]
    public class AzureStorageDeduplcatorTest
    {
        private readonly AzureStorageDeduplicator _deduplcator = new AzureStorageDeduplicator(new NoSqlTableInMemory<DuplicateEntity>());

        [SetUp]
        public void SetUp()
        {
        }

        [Test]
        public async Task EnsureNotDuplicateAsync()
        {
            var value = new byte[] {1, 2, 3 };

            var notDuplicate = await _deduplcator.EnsureNotDuplicateAsync(value);
            Assert.True(notDuplicate);

            notDuplicate = await _deduplcator.EnsureNotDuplicateAsync(value);
            Assert.False(notDuplicate);
        }
    }
}
