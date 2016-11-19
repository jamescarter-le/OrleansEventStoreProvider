using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreAdapterFactory : IQueueAdapterFactory, IQueueAdapter, IStreamFailureHandler
    {
        private string m_ProviderName;
        private Logger m_Logger;

        private string m_ConnectionString;

        private EventStoreQueueMapper m_QueueMapper;
        private IQueueAdapterCache m_QueueAdapterCache;
        private readonly ConcurrentDictionary<QueueId, EventStoreAdapterReceiver> m_Receivers = new ConcurrentDictionary<QueueId, EventStoreAdapterReceiver>();

        public string Name => m_ProviderName;
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (providerName == null) throw new ArgumentNullException(nameof(providerName));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));

            m_ProviderName = providerName;
            m_Logger = logger;

            m_QueueMapper = new EventStoreQueueMapper(1, providerName);
            m_QueueAdapterCache = new SimpleQueueAdapterCache(1000, m_Logger);

            m_ConnectionString = config.Properties["ConnectionString"];
        }

        public async Task<IQueueAdapter> CreateAdapter() => this;
        public IQueueAdapterCache GetQueueAdapterCache() => m_QueueAdapterCache;
        public IStreamQueueMapper GetStreamQueueMapper() => m_QueueMapper;

        public async Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return this;
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            // TODO: Publishing of Events is not implemented.
            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return m_Receivers.GetOrAdd(queueId, ConstructReceiver);
        }

        private EventStoreAdapterReceiver ConstructReceiver(QueueId queueId)
        {
            return new EventStoreAdapterReceiver(queueId, m_Logger, m_ConnectionString);
        }

        public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
            StreamSequenceToken sequenceToken)
        {
            // TODO: Delivery Failure is not implemented.
            return TaskDone.Done;
        }

        public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
            StreamSequenceToken sequenceToken)
        {
            // TODO: Subscription Failure is not implemented.
            return TaskDone.Done;
        }

        public bool ShouldFaultSubsriptionOnError => false;
    }
}
