using Orleans.Streams;

namespace Orleans.Providers.Streams.EventStore
{
    public class EventStoreQueueMapper : HashRingBasedStreamQueueMapper
    {
        public EventStoreQueueMapper(int nQueues, string queueNamePrefix) : base(nQueues, queueNamePrefix)
        {
        }
    }
}