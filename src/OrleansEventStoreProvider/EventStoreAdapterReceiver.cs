using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.EventStore
{
    public class EventStoreAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly QueueId m_QueueId;
        private readonly IEventStoreConnection m_EventStoreConnection;
        private readonly Logger m_Logger;

        private readonly ConcurrentDictionary<string, object> m_CachedSubscriptions = new ConcurrentDictionary<string, object>();
        private readonly ConcurrentQueue<IBatchContainer> m_MessagesAwaitingDelivery = new ConcurrentQueue<IBatchContainer>();

        public EventStoreAdapterReceiver(QueueId queueId, Logger logger, IEventStoreConnection connection)
        {
            m_QueueId = queueId;
            m_Logger = logger;
            m_EventStoreConnection = connection;
        }

        /// <summary>
        /// Open the EventStore Connection to the cluster.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task Initialize(TimeSpan timeout)
        {
        }

        /// <summary>
        /// Notify the AdapterRecevier that it should subscribe to an EventStore Stream.
        /// </summary>
        /// <param name="streamNamespace"></param>
        /// <param name="eventStoreStreamSequenceToken"></param>
        public void SubscribeTo(string streamNamespace, EventStoreStreamSequenceToken eventStoreStreamSequenceToken)
        {
            m_CachedSubscriptions.AddOrUpdate(streamNamespace, name => CreateEventStoreSubscription(name, eventStoreStreamSequenceToken), 
                (streamName, stream) => stream); // Update is a NO-OP.
        }

        /// <summary>
        /// Creates the Subscription with EventStore.
        /// </summary>
        /// <param name="streamName">The name of the EventStore Stream we are subscribing to.</param>
        /// <param name="eventStoreStreamSequenceToken"></param>
        /// <returns>An EventStoreSubscription or EventStorePersistentSubscription.</returns>
        private object CreateEventStoreSubscription(string streamName, EventStoreStreamSequenceToken eventStoreStreamSequenceToken)
        {
            object subscription = null;
            if (eventStoreStreamSequenceToken == null || eventStoreStreamSequenceToken.EventNumber == int.MinValue)
            {
                var subscribeTask = m_EventStoreConnection.SubscribeToStreamAsync(streamName, true, EventAppeared,
                    SubscriptionDropped);
                Task.WaitAll(subscribeTask);
                if (subscribeTask.IsFaulted)
                    throw subscribeTask.Exception;

                subscription = subscribeTask.Result;
            }
            else
            {
                // If we have been provided with a StreamSequenceToken, we can call SubscribeFrom to get previous messages.
                subscription = m_EventStoreConnection.SubscribeToStreamFrom(streamName,
                    eventStoreStreamSequenceToken.EventNumber, new CatchUpSubscriptionSettings(100, 20, false, true), EventAppeared);
            }

            m_Logger.Info($"Subscribed to Stream {streamName}");
            return subscription;
        }

        private void SubscriptionDropped(EventStoreSubscription eventStoreSubscription, SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {
            m_Logger.Info($"Subscription dropped for Stream {eventStoreSubscription.StreamId}: {subscriptionDropReason} \r\n {arg3}");
        }

        /// <summary>
        /// Add a ResolvedEvent to the Push Subscription type queue.
        /// </summary>
        /// <param name="subscription">The subscription for which the event is for.</param>
        /// <param name="resolvedEvent">The event we are being notified with by EventStore.</param>
        private void EventAppeared(EventStoreSubscription subscription, ResolvedEvent resolvedEvent)
        {
            EnqueueEvent(subscription.StreamId, resolvedEvent);
        }

        /// <summary>
        /// Add a ResolvedEvent to the Push Subscription type queue.
        /// </summary>
        /// <param name="subscription">The subscription for which the event is for.</param>
        /// <param name="resolvedEvent">The event we are being notified with by EventStore.</param>
        private void EventAppeared(EventStoreCatchUpSubscription subscription, ResolvedEvent resolvedEvent)
        {
            m_Logger.Info("EventNumber:" + resolvedEvent.Event.EventNumber);
            EnqueueEvent(subscription.StreamId, resolvedEvent);
        }

        private void EnqueueEvent(string streamId, ResolvedEvent resolvedEvent)
        {
            var sequenceToken = new EventStoreStreamSequenceToken(resolvedEvent.Event.EventNumber);
            m_MessagesAwaitingDelivery.Enqueue(new EventStoreBatchContainer(Guid.Empty, streamId, sequenceToken, resolvedEvent));
        }

        /// <summary>
        /// We pull the ResolvedEvents from the ConcurrentQueue if any exist for the Push Subscription type subscriptions.
        /// </summary>
        /// <param name="maxCount">The maximum number of events to resolve.</param>
        /// <returns>A list of EventStoreBatchContainers with new messages.</returns>
        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var containerList = new List<IBatchContainer>();
            if (maxCount == 0)
                return containerList;

            IBatchContainer container;
            while (containerList.Count < maxCount && m_MessagesAwaitingDelivery.TryDequeue(out container))
            {
                containerList.Add(container);
            }

            // Once we have pulled from the Queue the Push type messages, we can poll the Pull type subscriptions.
            // TODO: Not yet implemented Pull subscriptions - we may need to override the QueueCache and use that, as EventStore IS effectively the cache.
            // TODO: We many end up not needing the AdapterReceiver at all...

            return containerList;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            await AcknowledgeEvents(messages);
        }

        /// <summary>
        /// If the Stream the message is from is a Persistent Subscription we notify the server to update our processed messages.
        /// </summary>
        /// <param name="messages">The messages to acknowledge with the EventStore cluster.</param>
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

        /// <summary>
        /// Unsubscribe from all open streams.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Task Shutdown(TimeSpan timeout)
        {
            foreach (var stream in m_CachedSubscriptions.Values)
            {
                (stream as EventStorePersistentSubscription)?.Stop(timeout);
                (stream as EventStoreStreamCatchUpSubscription)?.Stop(timeout);
            }

            return TaskDone.Done;
        }

        // Push a Control Message through to the QueueCache for this Receiver to kick the PullingAgent to grab a QueueCursor.
        public void Start(Guid id, string streamNamespace)
        {
            m_MessagesAwaitingDelivery.Enqueue(new BatchContainerControlMessage(id, streamNamespace, new EventStoreStreamSequenceToken(int.MinValue)));
        }
    }
}