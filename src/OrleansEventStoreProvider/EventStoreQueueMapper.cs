using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreQueueMapper : HashRingBasedStreamQueueMapper, IStreamQueueMapper
    {
        private readonly ConcurrentDictionary<QueueId, List<string>> m_QueueStreamNames = new ConcurrentDictionary<QueueId, List<string>>();
        private readonly ConcurrentDictionary<QueueId, EventStoreAdapterReceiver> m_QueueReceivers = new ConcurrentDictionary<QueueId, EventStoreAdapterReceiver>();

        public EventStoreQueueMapper(int nQueues, string queueNamePrefix) : base(nQueues, queueNamePrefix)
        {
        }

        //QueueId IStreamQueueMapper.GetQueueForStream(Guid streamGuid, string streamNamespace)
        //{
        //    var queueId = base.GetQueueForStream(streamGuid, streamNamespace);
        //    m_QueueStreamNames.AddOrUpdate(queueId, x => new List<string> { streamNamespace }, (x, y) =>
        //    {
        //        if (!y.Contains(streamNamespace))
        //        {
        //            y.Add(streamNamespace);
        //        }
        //        return y;
        //    });


        //    EventStoreAdapterReceiver receiver;
        //    if (m_QueueReceivers.TryGetValue(queueId, out receiver))
        //    {
        //        receiver.SubscribeTo(streamNamespace);
        //    }

        //    return queueId;
        //}

        ///// <summary>
        ///// This is never called because the method is not virtual and it is invoked on IConsistentRingStreamQueueMapper
        ///// </summary>
        ///// <param name="streamGuid"></param>
        ///// <param name="streamNamespace"></param>
        ///// <returns></returns>
        //public new QueueId GetQueueForStream(Guid streamGuid, string streamNamespace)
        //{
        //    var queueId = base.GetQueueForStream(streamGuid, streamNamespace);
        //    m_QueueStreamNames.AddOrUpdate(queueId, x => new List<string> {streamNamespace}, (x, y) =>
        //    {
        //        if (!y.Contains(streamNamespace))
        //        {
        //            y.Add(streamNamespace);
        //        }
        //        return y;
        //    });


        //    EventStoreAdapterReceiver receiver;
        //    if (m_QueueReceivers.TryGetValue(queueId, out receiver))
        //    {
        //        receiver.SubscribeTo(streamNamespace);
        //    }

        //    return queueId;
        //}

        //public void AssociateReceiverWithQueue(QueueId queueId, EventStoreAdapterReceiver receiver)
        //{
        //    m_QueueReceivers.AddOrUpdate(queueId, x => receiver, (x, y) => receiver);
        //}

        //public IEnumerable<string> GetStreamNamesForQueue(QueueId queueId)
        //{
        //    List<string> streamNames;
        //    if (m_QueueStreamNames.TryGetValue(queueId, out streamNames))
        //    {
        //        return streamNames;
        //    }

        //    return new string[0];
        //}
    }
}