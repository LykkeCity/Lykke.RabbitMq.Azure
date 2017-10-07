using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;
using MessagePack;

namespace Lykke.RabbitMq.Azure
{
    public sealed class MessagePackBlobPublishingQueueRepository : IPublishingQueueRepository
    {
        private const string Container = "RabbitMqPublisherMessages-v-3-1-mp";

        private readonly IBlobStorage _storage;
        private readonly string _instanceName;

        /// <param name="storage"></param>
        /// <param name="instanceName">Instance name, required when multiple publishers publishes to single exchange</param>
        public MessagePackBlobPublishingQueueRepository(IBlobStorage storage, string instanceName = null)
        {
            _storage = storage;
            _instanceName = instanceName;
        }

        /// <summary>
        /// Saves messages to azure storage
        /// </summary>
        /// <param name="items">Serialized messages</param>
        /// <param name="exchangeName">Exchange name</param>
        /// <returns></returns>
        public async Task SaveAsync(IReadOnlyCollection<byte[]> items, string exchangeName)
        {
            using (var stream = new MemoryStream())
            {
                MessagePackSerializer.Serialize(stream, items);

                await _storage.SaveBlobAsync(Container, GetKey(exchangeName), stream);
            }
        }
        
        public async Task<IReadOnlyCollection<byte[]>> LoadAsync(string exchangeName)
        {
            if (!await _storage.HasBlobAsync(Container, GetKey(exchangeName)))
            {
                return null;
            }

            using (var stream = await _storage.GetAsync(Container, GetKey(exchangeName)))
            {
                return MessagePackSerializer.Deserialize<IReadOnlyCollection<byte[]>>(stream);
            }
        }

        private string GetKey(string exchangeName)
        {
            return _instanceName != null
                ? $"{exchangeName}.{_instanceName}"
                : exchangeName;
        }
    }
}
