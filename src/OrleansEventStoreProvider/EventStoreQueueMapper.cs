using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreQueueMapper : HashRingBasedStreamQueueMapper
    {
        private readonly ConcurrentDictionary<QueueId, string> m_QueueStreamNames =
            new ConcurrentDictionary<QueueId, string>();

        public EventStoreQueueMapper(int nQueues, string queueNamePrefix) : base(nQueues, queueNamePrefix)
        {
        }

        public new QueueId GetQueueForStream(Guid streamGuid, string streamNamespace)
        {
            var queueId = base.GetQueueForStream(streamGuid, streamNamespace);
            m_QueueStreamNames.AddOrUpdate(queueId, x => streamNamespace, (x, y) => streamNamespace);
            return queueId;
        }

        public string GetStreamsForQueue(QueueId queueId)
        {
            string streamName;
            if (!m_QueueStreamNames.TryGetValue(queueId, out streamName))
            {
                throw new Exception("Could not find Stream name, are we cross silo?");
            }
            return streamName;
        }
    }
}