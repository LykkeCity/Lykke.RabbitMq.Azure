using Lykke.AzureStorage.Tables;

namespace Lykke.RabbitMq.Azure
{
    public sealed class DeferredMessageEntity : AzureTableEntity
    {
        public byte[] Message { get; set; }
    }
}
