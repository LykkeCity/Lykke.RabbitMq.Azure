using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzureStorage;
using MessagePack;

namespace Lykke.RabbitMq.Azure
{
    [Obsolete("Should be removed with next release")]
    internal sealed class MessagePackBlobPublishingQueueV31Repository
    {
        private const string Container = "rabbitmqpublishermessages-v-3-1-mp";

        private readonly IBlobStorage _storage;
        private readonly string _instanceName;

        public MessagePackBlobPublishingQueueV31Repository(IBlobStorage storage, string instanceName = null)
        {
            _storage = storage;
            _instanceName = instanceName;
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
