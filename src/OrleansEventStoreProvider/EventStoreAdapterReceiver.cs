using System;
using System.Collections.Concurrent;
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
        private readonly IQueueCache m_QueueCache;
        private ConcurrentBag<Task> m_SubscribeTasks = new ConcurrentBag<Task>();
        private HashSet<string> m_SubscribedStreams = new HashSet<string>();

        public EventStoreAdapterReceiver(IEventStoreConnection eventStoreConnection, Logger logger, QueueId queueId, EventStoreQueueMapper queueMapper, IQueueAdapterCache queueAdapterCache)
        {
            m_EventStoreConnection = eventStoreConnection;
            m_Logger = logger;
            m_QueueId = queueId;
            m_QueueMapper = queueMapper;
            m_QueueCache = queueAdapterCache.CreateQueueCache(queueId);
        }

        public async Task Initialize(TimeSpan timeout)
        {
            m_QueueMapper.AssociateReceiverWithQueue(m_QueueId, this);
        }

        public void SubscribeTo(string streamNamespace)
        {
            m_SubscribedStreams.Add(streamNamespace);
            m_SubscribeTasks.Add(Task.Run(async () =>
            {
                await m_EventStoreConnection.SubscribeToStreamAsync(streamNamespace, true, EventAppeared, SubscriptionDropped);
                m_Logger.Info($"Subscribed to Stream {streamNamespace}");
            }));
        }

        private void SubscriptionDropped(EventStoreSubscription eventStoreSubscription, SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {
            m_Logger.Info($"Subscription dropped for Stream {eventStoreSubscription.StreamId}: {subscriptionDropReason} \r\n {arg3}");
        }

        private void EventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            var sequenceToken = new EventStoreStreamSequenceToken(resolvedEvent.Event.EventNumber);
            m_QueueCache.AddToCache(new [] {
                new EventStoreBatchContainer(Guid.Empty, eventStoreSubscription.StreamId, sequenceToken, resolvedEvent.Event.Data)
            });
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var queueStreams = m_QueueMapper.GetStreamNamesForQueue(m_QueueId);
            foreach (var queueStream in queueStreams)
            {
                if (!m_SubscribedStreams.Contains(queueStream))
                {
                    SubscribeTo(queueStream);
                }
            }

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