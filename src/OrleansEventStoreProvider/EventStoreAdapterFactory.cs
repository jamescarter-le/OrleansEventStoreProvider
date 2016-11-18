using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;
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

        private IEventStoreConnection m_EventStoreConnection;
        private bool m_IsConnected = false;

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

            m_QueueMapper = new EventStoreQueueMapper(2, providerName);
            m_QueueAdapterCache = new SimpleQueueAdapterCache(100, m_Logger);

            var connectionString = config.Properties["ConnectionString"];
            ConstructEventStoreConnection(connectionString);
        }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            if(!m_IsConnected)
                await m_EventStoreConnection.ConnectAsync();

            return this;
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return m_QueueAdapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return m_QueueMapper;
        }

        public async Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return this;
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return m_Receivers.GetOrAdd(queueId, ConstructReceiver);
        }

        private EventStoreAdapterReceiver ConstructReceiver(QueueId queueId)
        {
            return new EventStoreAdapterReceiver(m_EventStoreConnection, m_Logger);
        }

        private void ConstructEventStoreConnection(string connectionString)
        {
            m_EventStoreConnection = EventStoreConnection.Create(connectionString);
            m_EventStoreConnection.Connected += EventStoreConnection_Connected;
            m_EventStoreConnection.Disconnected += EventStoreConnection_Disconnected;
            m_EventStoreConnection.AuthenticationFailed += EventStoreConnection_AuthenticationFailed;
            m_EventStoreConnection.Closed += EventStoreConnection_Closed;
            m_EventStoreConnection.ErrorOccurred += EventStoreConnection_ErrorOccurred;
            m_EventStoreConnection.Reconnecting += EventStoreConnection_Reconnecting;
        }

        #region EventStoreEvents
        private void EventStoreConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
        {
            m_Logger.Info($"Adapter for EventStore {m_ProviderName} is reconnecting.");
        }

        private void EventStoreConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            m_Logger.Error(1, $"Adapter for EventStore {m_ProviderName} received an error: {e.Exception}", e.Exception);
        }

        private void EventStoreConnection_Closed(object sender, ClientClosedEventArgs e)
        {
            m_Logger.Info($"Adapter for EventStore {m_ProviderName} closed connection: {e.Reason}");
        }

        private void EventStoreConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            m_Logger.Info($"Adapter for EventStore {m_ProviderName} failed authentication: {e.Reason}");
        }

        private void EventStoreConnection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Adapter for EventStore {m_ProviderName} disconnected from {e.RemoteEndPoint}");
        }

        private void EventStoreConnection_Connected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Adapter for EventStore {m_ProviderName} connected to {e.RemoteEndPoint}");
        }
        #endregion

        public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
            StreamSequenceToken sequenceToken)
        {
            return TaskDone.Done;
        }

        public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
            StreamSequenceToken sequenceToken)
        {
            return TaskDone.Done;
        }

        public bool ShouldFaultSubsriptionOnError => false;
    }
}
