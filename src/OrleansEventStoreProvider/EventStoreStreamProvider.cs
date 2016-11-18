using System;
using System.Reflection;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreStreamProvider : PersistentStreamProvider<EventStoreAdapterFactory>, IStreamProvider
    {
        private EventStoreAdapterFactory m_CachedAdapterFactory;

        protected EventStoreAdapterFactory AdapterFactory
        {
            get
            {
                if (m_CachedAdapterFactory == null)
                {
                    var accessor = this.GetType().BaseType.GetField("adapterFactory", BindingFlags.Instance | BindingFlags.NonPublic);
                    m_CachedAdapterFactory = (EventStoreAdapterFactory) accessor.GetValue(this);
                }
                return m_CachedAdapterFactory;
            }
        }


        IAsyncStream<T> IStreamProvider.GetStream<T>(Guid id, string streamNamespace)
        {
            var queueId = AdapterFactory.GetStreamQueueMapper().GetQueueForStream(id, streamNamespace);
            var receiver = (EventStoreAdapterReceiver)AdapterFactory.CreateReceiver(queueId);
            receiver.SubscribeTo(streamNamespace);

            var stream = base.GetStream<T>(id, streamNamespace);
            return stream;
        }
    }
}