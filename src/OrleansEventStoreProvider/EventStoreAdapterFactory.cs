using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.EventStore
{
    public class EventStoreAdapterFactory : IQueueAdapterFactory, IQueueAdapter, IStreamFailureHandler
    {
        private string m_ProviderName;
        private Logger m_Logger;

        private string m_ConnectionString;
        private IEventStoreConnection m_EventStoreConnection;

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
            m_QueueAdapterCache = new EventStoreQueueAdapterCache(this, m_Logger);

            m_ConnectionString = config.Properties["ConnectionString"];
            CreateConnection();
        }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            await m_EventStoreConnection.ConnectAsync();
            return this;
        }

        public IQueueAdapterCache GetQueueAdapterCache() => m_QueueAdapterCache;
        public IStreamQueueMapper GetStreamQueueMapper() => m_QueueMapper;

        public async Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return this;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            var eventData = events.Select(x => new EventData(Guid.NewGuid(), streamGuid.ToString(), true, x.ToJsonBytes(), null));
            await m_EventStoreConnection.AppendToStreamAsync(streamNamespace, ExpectedVersion.Any, eventData);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return m_Receivers.GetOrAdd(queueId, ConstructReceiver);
        }

        private EventStoreAdapterReceiver ConstructReceiver(QueueId queueId)
        {
            return new EventStoreAdapterReceiver(queueId, m_Logger, m_EventStoreConnection);
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

        public void CreateConnection()
        {
            m_EventStoreConnection = EventStoreConnection.Create(m_ConnectionString);
            m_EventStoreConnection.Connected += EventStoreConnection_Connected;
            m_EventStoreConnection.Disconnected += EventStoreConnection_Disconnected;
            m_EventStoreConnection.AuthenticationFailed += EventStoreConnection_AuthenticationFailed;
            m_EventStoreConnection.Closed += EventStoreConnection_Closed;
            m_EventStoreConnection.ErrorOccurred += EventStoreConnection_ErrorOccurred;
            m_EventStoreConnection.Reconnecting += EventStoreConnection_Reconnecting;
        }

        private void EventStoreConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
        {
            m_Logger.Info($"Reconnecting.");
        }

        private void EventStoreConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            m_Logger.Error(1, $"Received an error: {e.Exception}", e.Exception);
        }

        private void EventStoreConnection_Closed(object sender, ClientClosedEventArgs e)
        {
            m_Logger.Info($"Closed connection: {e.Reason}");
        }

        private void EventStoreConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            m_Logger.Info($"Failed authentication: {e.Reason}");
        }

        private void EventStoreConnection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Disconnected from {e.RemoteEndPoint}");
        }

        private void EventStoreConnection_Connected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Connected to {e.RemoteEndPoint}");
        }
    }
}
