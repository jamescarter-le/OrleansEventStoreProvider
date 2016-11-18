using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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

        private readonly ConcurrentDictionary<string, object> m_CachedSubscriptions = new ConcurrentDictionary<string, object>();
        private ConcurrentQueue<EventStoreBatchContainer> m_MessagesAwaitingDelivery = new ConcurrentQueue<EventStoreBatchContainer>();

        private ConcurrentBag<Task> m_SubscribeTasks = new ConcurrentBag<Task>();
        private HashSet<string> m_SubscribedStreams = new HashSet<string>();

        public EventStoreAdapterReceiver(IEventStoreConnection eventStoreConnection, Logger logger)
        {
            m_EventStoreConnection = eventStoreConnection;
            m_Logger = logger;
        }

        public async Task Initialize(TimeSpan timeout)
        {
            //m_QueueMapper.AssociateReceiverWithQueue(m_QueueId, this);
        }

        public void SubscribeTo(string streamNamespace)
        {
            m_SubscribedStreams.Add(streamNamespace);
            m_SubscribeTasks.Add(Task.Run(async () =>
            {
                var subscription = await m_EventStoreConnection.SubscribeToStreamAsync(streamNamespace, true, EventAppeared, SubscriptionDropped);
                m_CachedSubscriptions.GetOrAdd(streamNamespace, subscription);
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
            m_MessagesAwaitingDelivery.Enqueue(new EventStoreBatchContainer(Guid.Empty, eventStoreSubscription.StreamId, sequenceToken, resolvedEvent));
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var containerList = new List<IBatchContainer>();
            if (maxCount == 0)
                return containerList;

            EventStoreBatchContainer container;
            while (containerList.Count < maxCount && m_MessagesAwaitingDelivery.TryDequeue(out container))
            {
                containerList.Add(container);
            }

            return containerList;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            await AcknowledgeEvents(messages);
        }

        private async Task AcknowledgeEvents(IList<IBatchContainer> messages)
        {
            var byNamespaces = messages.GroupBy(x => x.StreamNamespace);
            foreach (var byNamespace in byNamespaces)
            {
                var persistentSubscription = m_CachedSubscriptions[byNamespace.Key] as EventStorePersistentSubscription;
                if (persistentSubscription != null)
                {
                    persistentSubscription.Acknowledge(byNamespace.Cast<EventStoreBatchContainer>().Select(x => x.ResolvedEvent));
                }
            }
        }

        public Task Shutdown(TimeSpan timeout)
        {
            return TaskDone.Done;
        }
    }
}