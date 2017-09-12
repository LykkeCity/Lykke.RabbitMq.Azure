using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;
using Lykke.RabbitMqBroker.Subscriber;
using Newtonsoft.Json;

namespace Lykke.RabbitMq.Azure
{
    public class BlobPublishingQueueRepository<TMessageModel, TMessageAbstraction> : IPublishingQueueRepository<TMessageAbstraction>
        where TMessageModel : TMessageAbstraction, new()
    {
        private const string Container = "RabbitMqPublisherMessages";
        
        private readonly IBlobStorage _storage;
        private readonly RabbitMqSubscriptionSettings _settings;
        private readonly string _instanceName;

        /// <param name="storage"></param>
        /// <param name="settings"></param>
        /// <param name="instanceName">Instance name, required when multiple publishers publishes to single exchange</param>
        public BlobPublishingQueueRepository(IBlobStorage storage, RabbitMqSubscriptionSettings settings, string instanceName = null)
        {
            _storage = storage;
            _settings = settings;
            _instanceName = instanceName;
        }

        public async Task SaveAsync(IReadOnlyCollection<TMessageAbstraction> items)
        {
            using (var stream = new MemoryStream())
            using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
            using (var jsonWriter = new JsonTextWriter(streamWriter))
            {
                var serializer = new JsonSerializer();
                
                serializer.Serialize(jsonWriter, items);

                await jsonWriter.FlushAsync();
                await streamWriter.FlushAsync();
                await stream.FlushAsync();

                stream.Seek(0, SeekOrigin.Begin);

                await _storage.SaveBlobAsync(Container, GetKey(), stream);
            }
        }

        public async Task<IReadOnlyCollection<TMessageAbstraction>> LoadAsync()
        {
            if (!await _storage.HasBlobAsync(Container, GetKey()))
            {
                return null;
            }

            using (var stream = await _storage.GetAsync(Container, GetKey()))
            using (var streamReader = new StreamReader(stream, Encoding.UTF8))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                await stream.FlushAsync();

                stream.Seek(0, SeekOrigin.Begin);

                var serializer = new JsonSerializer();

                return serializer.Deserialize<TMessageModel[]>(jsonReader)
                    .Cast<TMessageAbstraction>()
                    .ToArray();
            }
        }

        private string GetKey()
        {
            return _instanceName != null
                ? $"{_settings.ExchangeName}.{_instanceName}"
                : _settings.ExchangeName;
        }
    }
}
