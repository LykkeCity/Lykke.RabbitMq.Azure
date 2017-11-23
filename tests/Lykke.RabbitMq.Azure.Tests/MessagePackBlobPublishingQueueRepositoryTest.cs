using System.Threading.Tasks;
using AzureStorage.Blob;
using NUnit.Framework;

namespace Lykke.RabbitMq.Azure.Tests
{
    [TestFixture]
    internal sealed class MessagePackBlobPublishingQueueRepositoryTests
    {
        private AzureBlobInMemory _azureBlobStorage;

        [SetUp]
        public void SetUp()
        {
            _azureBlobStorage = new AzureBlobInMemory();
        }

        [Test]
        public async Task ShouldDeserializeSerialized()
        {
            // Arrange
            var repo = new MessagePackBlobPublishingQueueRepository(_azureBlobStorage, "InstanceName");

            var expected = new[] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } };

            const string exchangeName = "ExchangeName";

            await repo.SaveAsync(expected, exchangeName);
            
            // Act
            var actual = await repo.LoadAsync(exchangeName);

            // Assert
            Assert.That(expected, Is.EqualTo(actual));
        }
    }
}
