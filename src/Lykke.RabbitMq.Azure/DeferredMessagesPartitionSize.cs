using JetBrains.Annotations;

namespace Lykke.RabbitMq.Azure
{
    /// <summary>
    /// Specifies the deferred messages partition size
    /// </summary>
    /// <remarks>
    /// If your publishes about 70 messages per minute and in the average the message is deferred about 1 hour, 
    /// <see cref="Minute"/> is a good choice. Because a single partition will 
    /// contains about 70 messages, and there will be about 60 partitions of not overdue messages in the storage
    /// </remarks>
    [PublicAPI]
    public enum DeferredMessagesPartitionSize
    {
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month
    };
}
