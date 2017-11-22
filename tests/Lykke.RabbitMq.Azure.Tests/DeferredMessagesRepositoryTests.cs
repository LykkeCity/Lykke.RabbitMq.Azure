using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Moq;
using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.RabbitMq.Azure.Tests
{
    [TestFixture]
    internal sealed class DeferredMessagesRepositoryTests
    {
        private Mock<INoSQLTableStorage<DeferredMessageEntity>> _storageMock;

        [SetUp]
        public void Setup()
        {
            _storageMock = new Mock<INoSQLTableStorage<DeferredMessageEntity>>();
        }

        [Test]
        public async Task Test_that_up_to_1000_messages_can_be_deferred_for_the_same_deliver_date()
        {
            // Arrange
            var savedPartitionKeys = new HashSet<string>();
            var savedRowKeys = new HashSet<string>();
            var repo = new DeferredMessagesRepository(_storageMock.Object);
            var deliverAt = new DateTime(2017, 11, 20, 15, 10, 45);

            _storageMock
                .Setup(x => x.InsertAsync(It.IsAny<DeferredMessageEntity>(), It.IsAny<int[]>()))
                .Returns(
                    (DeferredMessageEntity e, int[] p) =>
                    {
                        savedPartitionKeys.Add(e.PartitionKey);
                        savedRowKeys.Add(e.RowKey);

                        return Task.CompletedTask;
                    });

            // Act
            for (int i = 0; i < 1010; ++i)
            {
                await repo.SaveAsync(Array.Empty<byte>(), deliverAt);
            }

            // Assert
            Assert.AreEqual(1, savedPartitionKeys.Count);
            Assert.AreEqual(1000, savedRowKeys.Count);

            _storageMock.Verify(x => x.InsertAsync(It.IsAny<DeferredMessageEntity>(), It.IsAny<int[]>()), Times.Exactly(1010));
        }

        [Test]
        [TestCase(DeferredMessagesPartitionSize.Second, "2017-11-16T15-10-45")]
        [TestCase(DeferredMessagesPartitionSize.Minute, "2017-11-16T15-10-00")]
        [TestCase(DeferredMessagesPartitionSize.Hour, "2017-11-16T15-00-00")]
        [TestCase(DeferredMessagesPartitionSize.Day, "2017-11-16T00-00-00")]
        [TestCase(DeferredMessagesPartitionSize.Week, "2017-11-13T00-00-00")]
        [TestCase(DeferredMessagesPartitionSize.Month, "2017-11-01T00-00-00")]
        public async Task Test_partition_sizes(DeferredMessagesPartitionSize partitionSize, string expectedPartitionKey)
        {
            // Arrange
            var repo = new DeferredMessagesRepository(_storageMock.Object, partitionSize);
            var deliverAt = new DateTime(2017, 11, 16, 15, 10, 45, 123, DateTimeKind.Utc);
            string savedPartitionKey = null;

            _storageMock
                .Setup(x => x.InsertAsync(It.IsAny<DeferredMessageEntity>(), It.IsAny<int[]>()))
                .Returns(
                    (DeferredMessageEntity e, int[] p) =>
                    {
                        savedPartitionKey = e.PartitionKey;

                        return Task.CompletedTask;
                    });

            // Act
            await repo.SaveAsync(Array.Empty<byte>(), deliverAt);

            // Assert
            Assert.AreEqual(expectedPartitionKey, savedPartitionKey);
        }

        [Test]
        public async Task Test_that_deferred_message_envelope_key_is_builded_and_parsed_well()
        {
            // Arrange
            var repo = new DeferredMessagesRepository(_storageMock.Object);
            var now = new DateTime(2017, 11, 16, 15, 10, 45, 123, DateTimeKind.Utc);
            var partitionKey = "pk";
            var rowKey = "rk";

            _storageMock
                .Setup(x => x.ExecuteAsync(
                    It.IsAny<TableQuery<DeferredMessageEntity>>(),
                    It.IsAny<Action<IEnumerable<DeferredMessageEntity>>>(),
                    It.IsAny<Func<bool>>()))
                .Returns(
                    (TableQuery<DeferredMessageEntity> q, Action<IEnumerable<DeferredMessageEntity>> a, Func<bool> f) =>
                    {
                        a(new[]
                        {
                            new DeferredMessageEntity
                            {
                                PartitionKey = partitionKey,
                                RowKey = rowKey,
                                Message = Array.Empty<byte>()
                            }
                        });

                        return Task.CompletedTask;
                    });

            // Act 1
            var messages = await repo.GetOverdueMessagesAsync(now);
            
            // Assert 1
            Assert.NotNull(messages);
            Assert.AreEqual(1, messages.Count);

            // Act 2
            await repo.RemoveAsync(messages.Single().Key);

            // Assert 2
            _storageMock.Verify(x => x.DeleteAsync(It.Is<string>(pk => pk == partitionKey), It.Is<string>(rk => rk == rowKey)), Times.Once);
        }
    }
}
