using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;

namespace Lykke.RabbitMq.Azure
{
    public sealed class BlobPublishingQueueRepository : IPublishingQueueRepository
    {
        private const string Container = "RabbitMqPublisherMessages";

        private readonly IBlobStorage _storage;
        private readonly string _instanceName;

        /// <param name="storage"></param>
        /// <param name="instanceName">Instance name, required when multiple publishers publishes to single exchange</param>
        public BlobPublishingQueueRepository(IBlobStorage storage, string instanceName = null)
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
                var formater = new BinaryFormatter();
                formater.Serialize(stream, items.ToArray());
                await _storage.SaveBlobAsync(Container, GetKey(exchangeName), stream);
            }
        }


        public async Task<IReadOnlyCollection<byte[]>> LoadAsync(string exchangeName)
        {
            if (!await _storage.HasBlobAsync(Container, GetKey(exchangeName)))
            {
                return new byte[0][];
            }

            using (var stream = await _storage.GetAsync(Container, GetKey(exchangeName)))
            {
                var formater = new BinaryFormatter();
                var result = (IReadOnlyCollection<byte[]>)(byte[][])formater.Deserialize(stream);
                return result;
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
