using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreQueueMapper : HashRingBasedStreamQueueMapper
    {
        public EventStoreQueueMapper(int nQueues, string queueNamePrefix) : base(nQueues, queueNamePrefix)
        {
        }
    }
}