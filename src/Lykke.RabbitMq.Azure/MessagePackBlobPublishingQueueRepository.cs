using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;
using MessagePack;

namespace Lykke.RabbitMq.Azure
{
    public sealed class MessagePackBlobPublishingQueueRepository : IPublishingQueueRepository
    {
        private const string Container = "rabbitmqpublishermessages-v-5-0-mp";

        private readonly IBlobStorage _storage;
        private readonly string _instanceName;

        private readonly MessagePackBlobPublishingQueueV31Repository _legacyRepository;

        /// <param name="storage"></param>
        /// <param name="instanceName">Instance name, required when multiple publishers publishes to single exchange</param>
        public MessagePackBlobPublishingQueueRepository(IBlobStorage storage, string instanceName = null)
        {
            _storage = storage;
            _instanceName = instanceName;

            _legacyRepository = new MessagePackBlobPublishingQueueV31Repository(storage, instanceName);
        }

        /// <summary>
        /// Saves messages to azure storage
        /// </summary>
        /// <param name="items">Serialized messages</param>
        /// <param name="exchangeName">Exchange name</param>
        /// <returns></returns>
        public async Task SaveAsync(IReadOnlyCollection<RawMessage> items, string exchangeName)
        {
            using (var stream = new MemoryStream())
            {
                MessagePackSerializer.Serialize(stream, items.Select(i => new PublishingQueueMessage
                {
                    Body = i.Body,
                    RoutingKey = i.RoutingKey
                }));

                await _storage.SaveBlobAsync(Container, GetKey(exchangeName), stream);
            }
        }

        public async Task<IReadOnlyCollection<RawMessage>> LoadAsync(string exchangeName)
        {
            if (!await _storage.HasBlobAsync(Container, GetKey(exchangeName)))
            {
                var legacyMessages = await _legacyRepository.LoadAsync(exchangeName);

                return legacyMessages?
                    .Select(m => new RawMessage(m, null))
                    .ToArray();
            }

            using (var stream = await _storage.GetAsync(Container, GetKey(exchangeName)))
            {
                var messages = MessagePackSerializer.Deserialize<PublishingQueueMessage[]>(stream);

                return messages
                    .Select(m => new RawMessage(m.Body, m.RoutingKey))
                    .ToArray();
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
