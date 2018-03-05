using MessagePack;

namespace Lykke.RabbitMq.Azure
{
    [MessagePackObject]
    public sealed class PublishingQueueMessage
    {
        [Key(0)]
        public byte[] Body { get; set; }

        [Key(1)]
        public string RoutingKey { get; set; }
    }
}
