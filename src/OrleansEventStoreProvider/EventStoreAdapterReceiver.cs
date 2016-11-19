using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly QueueId m_QueueId;
        private IEventStoreConnection m_EventStoreConnection;
        private readonly Logger m_Logger;

        private readonly ConcurrentDictionary<string, object> m_CachedSubscriptions = new ConcurrentDictionary<string, object>();
        private readonly ConcurrentQueue<EventStoreBatchContainer> m_MessagesAwaitingDelivery = new ConcurrentQueue<EventStoreBatchContainer>();

        public EventStoreAdapterReceiver(QueueId queueId, Logger logger, string connectionString)
        {
            m_QueueId = queueId;
            m_Logger = logger;
            ConstructEventStoreConnection(connectionString);
        }

        /// <summary>
        /// Open the EventStore Connection to the cluster.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task Initialize(TimeSpan timeout)
        {
            await m_EventStoreConnection.ConnectAsync();
        }

        /// <summary>
        /// Notify the AdapterRecevier that it should subscribe to an EventStore Stream.
        /// </summary>
        /// <param name="streamNamespace"></param>
        public void SubscribeTo(string streamNamespace)
        {
            m_CachedSubscriptions.AddOrUpdate(streamNamespace, CreateEventStoreSubscription, 
                (streamName, stream) => stream); // Update is a NO-OP.
        }

        /// <summary>
        /// Creates the Subscription with EventStore.
        /// </summary>
        /// <param name="streamName">The name of the EventStore Stream we are subscribing to.</param>
        /// <returns>An EventStoreSubscription or EventStorePersistentSubscription.</returns>
        private object CreateEventStoreSubscription(string streamName)
        {
            var subscription = m_EventStoreConnection.SubscribeToStreamAsync(streamName, true, EventAppeared, SubscriptionDropped).Result;
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
        /// <param name="eventStoreSubscription">The subscription for which the event is for.</param>
        /// <param name="resolvedEvent">The event we are being notified with by EventStore.</param>
        private void EventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            var sequenceToken = new EventStoreStreamSequenceToken(resolvedEvent.Event.EventNumber);
            m_MessagesAwaitingDelivery.Enqueue(new EventStoreBatchContainer(Guid.Empty, eventStoreSubscription.StreamId, sequenceToken, resolvedEvent));
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

            EventStoreBatchContainer container;
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
                (stream as EventStoreSubscription)?.Unsubscribe();
            }

            return TaskDone.Done;
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
            m_Logger.Info($"Receiver {m_QueueId} is reconnecting.");
        }

        private void EventStoreConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            m_Logger.Error(1, $"Receiver {m_QueueId} received an error: {e.Exception}", e.Exception);
        }

        private void EventStoreConnection_Closed(object sender, ClientClosedEventArgs e)
        {
            m_Logger.Info($"Receiver {m_QueueId} closed connection: {e.Reason}");
        }

        private void EventStoreConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            m_Logger.Info($"Receiver {m_QueueId} failed authentication: {e.Reason}");
        }

        private void EventStoreConnection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Receiver {m_QueueId} disconnected from {e.RemoteEndPoint}");
        }

        private void EventStoreConnection_Connected(object sender, ClientConnectionEventArgs e)
        {
            m_Logger.Info($"Receiver {m_QueueId} connected to {e.RemoteEndPoint}");
        }
        #endregion
    }
}