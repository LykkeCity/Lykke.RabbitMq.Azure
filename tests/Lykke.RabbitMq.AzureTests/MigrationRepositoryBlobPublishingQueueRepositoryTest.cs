using System;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage.Blob;
using Lykke.RabbitMq.Azure;
using Lykke.RabbitMqBroker.Publisher;
using Lykke.RabbitMqBroker.Subscriber;
using NUnit.Framework;

namespace Lykke.RabbitMq.AzureTests
{
    [Obsolete("Used only for migration test")]
    [TestFixture]
    internal sealed class MigrationRepositoryBlobPublishingQueueRepositoryTest
    {
        public class TestMessage
        {
            public string Name { get; set; }
            public int Code { get; set; }

            public override bool Equals(object obj)
            {
                if (obj is TestMessage otherTestMessage)
                {
                    return Equals(otherTestMessage);
                }

                return false;
            }

            protected bool Equals(TestMessage other)
            {
                return string.Equals(Name, other.Name) && Code == other.Code;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ Code;
                }
            }

            public override string ToString()
            {
                return $"{Name}, {Code}";
            }
        }

        private const string ExchangeName = "ExchangeName";
        
        private AzureBlobInMemory _azureBlobStorage;
        private JsonMessageSerializer<TestMessage> _serializer;
        private JsonMessageDeserializer<TestMessage> _deserializer;

        [SetUp]
        public void SetUp()
        {
            _azureBlobStorage = new AzureBlobInMemory();
            _serializer = new JsonMessageSerializer<TestMessage>();
            _deserializer = new JsonMessageDeserializer<TestMessage>();
        }

        [Test]
        public async Task ShouldDeserializeOldFormat()
        {
            // Arrange
            // Makes AzureBlobInMemory happy
            await _azureBlobStorage.CreateContainerIfNotExistsAsync("RabbitMqPublisherMessages-v-3-1-mp");

            var expected = new[]
            {
                new TestMessage
                {
                    Name = "A",
                    Code = 1
                },
                new TestMessage
                {
                    Name = "B",
                    Code = 2
                }
            };

            var oldRepo = new V2BlobPublishingQueueRepository<TestMessage>(_azureBlobStorage, "InstanceName");

            await oldRepo.SaveAsync(expected, ExchangeName);

            var repo = new MigratingBlobPublishingQueueRepository<TestMessage>(
                _azureBlobStorage, 
                _serializer, 
                "InstanceName");

            // Act
            var loaded = await repo.LoadAsync(ExchangeName);
            var actual = loaded.Select(_deserializer.Deserialize).ToArray();

            // Assert
            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        public async Task ShouldPreferToDeserializeNewFormat()
        {
            // Arrange
            var oldMessages = new[]
            {
                new TestMessage
                {
                    Name = "A",
                    Code = 1
                },
                new TestMessage
                {
                    Name = "B",
                    Code = 2
                }
            };
            var expected = new[] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } };
            var oldRepo = new V2BlobPublishingQueueRepository<TestMessage>(_azureBlobStorage, "InstanceName");
            
            await oldRepo.SaveAsync(oldMessages, ExchangeName);

            var repo = new MigratingBlobPublishingQueueRepository<TestMessage>(
                _azureBlobStorage,
                _serializer,
                "InstanceName");

            await repo.SaveAsync(expected, ExchangeName);

            // Act
            var actual = await repo.LoadAsync(ExchangeName);

            // Assert
            Assert.That(expected, Is.EqualTo(actual));
        }
    }
}
