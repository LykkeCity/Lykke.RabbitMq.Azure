using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;

namespace Lykke.RabbitMq.Azure
{
    [Obsolete("Use this repository only for migration from 2.x to 3.1 MessagePackBlobPublishingQueueRepository data format")]
    public sealed class MigratingBlobPublishingQueueRepository<TMessage> : IPublishingQueueRepository
    {
        private readonly IRabbitMqSerializer<TMessage> _messageSerializer;
        private readonly V2BlobPublishingQueueRepository<TMessage> _oldFormatRepository;
        private readonly MessagePackBlobPublishingQueueRepository _newFormatRepository;

        public MigratingBlobPublishingQueueRepository(
            IBlobStorage storage,
            IRabbitMqSerializer<TMessage> messageSerializer,
            string instanceName = null)
        {
            _messageSerializer = messageSerializer;
            _oldFormatRepository = new V2BlobPublishingQueueRepository<TMessage>(storage, instanceName);
            _newFormatRepository = new MessagePackBlobPublishingQueueRepository(storage, instanceName);
        }

        public Task SaveAsync(IReadOnlyCollection<byte[]> items, string exchangeName)
        {
            return _newFormatRepository.SaveAsync(items, exchangeName);
        }

        public async Task<IReadOnlyCollection<byte[]>> LoadAsync(string exchangeName)
        {
            return await _newFormatRepository.LoadAsync(exchangeName) ?? await LoadOldFormatAsync(exchangeName);
        }

        private async Task<IReadOnlyCollection<byte[]>> LoadOldFormatAsync(string exchangeName)
        {
            var messages = await _oldFormatRepository.LoadAsync(exchangeName);

            return new ReadOnlyCollection<byte[]>(messages.Select(_messageSerializer.Serialize).ToArray());
        }
    }
}
