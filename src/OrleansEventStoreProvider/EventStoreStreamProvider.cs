using System;
using System.Reflection;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Providers.Streams.EventStore
{
    public class EventStoreStreamProvider : PersistentStreamProvider<EventStoreAdapterFactory>, IStreamProvider
    {
        private EventStoreAdapterFactory m_CachedAdapterFactory;

        /// <summary>
        /// Capture the AdapterFactory for this Provider
        /// </summary>
        /// <remarks>
        /// This accesses the private field "adapterFactory" - which I realise is nasty but there is no other way to resolve the AdapterFactory.
        /// If the IStreamProvider was passed to child IQueueAdapterFactory we could associate it this way.
        /// </remarks>
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

        /// <summary>
        /// We intercept the call to resolve a Stream and use this opportunity to notify the appropriate Queue to Subscribe to the EventStore stream.
        /// </summary>
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