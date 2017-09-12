using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.RabbitMqBroker.Publisher;
using Newtonsoft.Json;

namespace Lykke.RabbitMq.Azure
{
    public class BlobPublishingQueueRepository<TMessageImpl, TMessageAbstraction> : IPublishingQueueRepository<TMessageAbstraction>
        where TMessageImpl : TMessageAbstraction, new()
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

        public async Task SaveAsync(IReadOnlyCollection<TMessageAbstraction> items, string exchangeName)
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

                await _storage.SaveBlobAsync(Container, GetKey(exchangeName), stream);
            }
        }

        public async Task<IReadOnlyCollection<TMessageAbstraction>> LoadAsync(string exchangeName)
        {
            if (!await _storage.HasBlobAsync(Container, GetKey(exchangeName)))
            {
                return null;
            }

            using (var stream = await _storage.GetAsync(Container, GetKey(exchangeName)))
            using (var streamReader = new StreamReader(stream, Encoding.UTF8))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                await stream.FlushAsync();

                stream.Seek(0, SeekOrigin.Begin);

                var serializer = new JsonSerializer();

                return serializer.Deserialize<TMessageImpl[]>(jsonReader)
                    .Cast<TMessageAbstraction>()
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
