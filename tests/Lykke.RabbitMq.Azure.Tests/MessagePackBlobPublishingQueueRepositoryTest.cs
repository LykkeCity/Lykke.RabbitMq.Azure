using System.Linq;
using System.Threading.Tasks;
using AzureStorage.Blob;
using Lykke.RabbitMqBroker.Publisher;
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

            var expected = new[]
            {
                new RawMessage(new byte[] { 1, 2, 3 }, null),
                new RawMessage(new byte[] { 4, 5, 6 }, "route1")
            };

            const string exchangeName = "ExchangeName";

            await repo.SaveAsync(expected, exchangeName);
            
            // Act
            var actual = (await repo.LoadAsync(exchangeName)).ToArray();

            // Assert
            Assert.That(expected.Length, Is.EqualTo(actual.Length));
            Assert.That(expected[0].Body, Is.EqualTo(actual[0].Body));
            Assert.That(expected[0].RoutingKey, Is.EqualTo(actual[0].RoutingKey));
            Assert.That(expected[1].Body, Is.EqualTo(actual[1].Body));
            Assert.That(expected[1].RoutingKey, Is.EqualTo(actual[1].RoutingKey));
        }
    }
}
