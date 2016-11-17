using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IEventStoreConnection m_EventStoreConnection;
        private readonly Logger m_Logger;
        private readonly QueueId m_QueueId;
        private readonly EventStoreQueueMapper m_QueueMapper;
        private readonly string m_StreamNamespace;
        private readonly IQueueCache m_QueueCache;

        public EventStoreAdapterReceiver(IEventStoreConnection eventStoreConnection, Logger logger, QueueId queueId, EventStoreQueueMapper queueMapper, IQueueAdapterCache queueAdapterCache)
        {
            m_EventStoreConnection = eventStoreConnection;
            m_Logger = logger;
            m_QueueId = queueId;
            m_QueueMapper = queueMapper;
            m_StreamNamespace = m_QueueMapper.GetStreamsForQueue(queueId);
            m_QueueCache = queueAdapterCache.CreateQueueCache(queueId);
        }

        public async Task Initialize(TimeSpan timeout)
        {
            CancellationTokenSource cancel = new CancellationTokenSource(timeout);
            await Task.Run(async () =>
            {
                await m_EventStoreConnection.SubscribeToStreamAsync(m_StreamNamespace, true, EventAppeared, SubscriptionDropped);
                m_Logger.Info($"Subscribed to Stream {m_StreamNamespace}");
            }, cancel.Token);
        }

        private void SubscriptionDropped(EventStoreSubscription eventStoreSubscription, SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {
            m_Logger.Info($"Subscription dropped for Stream {m_QueueId.GetStringNamePrefix()}");
        }

        private void EventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            var sequenceToken = new EventStoreStreamSequenceToken(resolvedEvent.Event.EventNumber);
            m_QueueCache.AddToCache(new [] {
                new EventStoreBatchContainer(Guid.Empty, m_StreamNamespace, sequenceToken, resolvedEvent.Event.Data)
            });
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            return new List<IBatchContainer>();
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            //return null;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            return TaskDone.Done;
        }
    }
}