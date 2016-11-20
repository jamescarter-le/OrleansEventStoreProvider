using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.EventStore;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.EventStore
{
    public class EventStoreQueueAdapterCache : IQueueAdapterCache
    {
        private readonly EventStoreAdapterFactory m_AdapterFactory;
        private readonly Logger m_Logger;
        private readonly ConcurrentDictionary<QueueId, EventStoreQueueCache> m_QueueCaches = new ConcurrentDictionary<QueueId, EventStoreQueueCache>();

        public EventStoreQueueAdapterCache(EventStoreAdapterFactory adapterFactory, Logger logger)
        {
            m_AdapterFactory = adapterFactory;
            m_Logger = logger;
        }

        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return m_QueueCaches.GetOrAdd(queueId, ConstructQueueCache);
        }

        private EventStoreQueueCache ConstructQueueCache(QueueId queueId)
        {
            var receiver = (EventStoreAdapterReceiver)m_AdapterFactory.CreateReceiver(queueId);
            return new EventStoreQueueCache(100, m_Logger, queueId, receiver);
        }
    }

    public class EventStoreQueueCache : SimpleQueueCache, IQueueCache
    {
        private readonly QueueId m_QueueId;
        private readonly EventStoreAdapterReceiver m_Receiver;

        public EventStoreQueueCache(int cacheSize, Logger logger, QueueId queueId, EventStoreAdapterReceiver receiver)
            :base(cacheSize, logger)
        {
            m_QueueId = queueId;
            m_Receiver = receiver;
        }

        public virtual void AddToCache(IList<IBatchContainer> msgs)
        {
            msgs = msgs.OfType<EventStoreBatchContainer>().Cast<IBatchContainer>().ToList();
            base.AddToCache(msgs);
        }

        IQueueCacheCursor IQueueCache.GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
        {
            m_Receiver.SubscribeTo(streamIdentity.Namespace, token as EventStoreStreamSequenceToken);
            return base.GetCacheCursor(streamIdentity, token);
        }
    }
}
