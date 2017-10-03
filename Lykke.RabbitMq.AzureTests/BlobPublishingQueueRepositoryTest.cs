using System.Threading.Tasks;
using AzureStorage.Blob;
using Lykke.RabbitMq.Azure;
using NUnit.Framework;

namespace Lykke.RabbitMq.AzureTests
{
    [TestFixture, Explicit("Depends on local azure storage emulator")]
    internal sealed class BlobPublishingQueueRepositoryTest
    {
        private AzureBlobInMemory _azureBlobStorage;

        [SetUp]
        public void SetUp()
        {
            _azureBlobStorage = new AzureBlobInMemory();
        }

        [Test]
        public async Task ShouldSerialize()
        {
            var repo = new BlobPublishingQueueRepository(_azureBlobStorage, "InstanceName");

            var expected = new[] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } };

            const string exchangeName = "ExchangeName";

            await repo.SaveAsync(expected, exchangeName);
            var actual = await repo.LoadAsync(exchangeName);

            Assert.That(expected, Is.EqualTo(actual));
        }
    }
}
