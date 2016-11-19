using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    /// <summary>
    /// The sequence token for the EventStore Stream.
    /// </summary>
    /// <remarks>This must derive from EventSequenceToken to use the Orleans SimpleQueueAdapterCache.</remarks>
    public class EventStoreStreamSequenceToken : EventSequenceToken
    {
        /// <summary>
        /// The EventNumber of the EventStore ResolvedEvent.
        /// </summary>
        /// <remarks>This is always the child event number if the Event is a linkTo.</remarks>
        public int EventNumber { get; }

        public EventStoreStreamSequenceToken(int eventNumber) : base(eventNumber, eventNumber)
        {
            EventNumber = eventNumber;
        }

        public override bool Equals(StreamSequenceToken other)
        {
            return (other as EventStoreStreamSequenceToken)?.EventNumber == EventNumber;
        }

        public override int CompareTo(StreamSequenceToken other)
        {
            return EventNumber.CompareTo(((EventStoreStreamSequenceToken)other).EventNumber);
        }
    }
}