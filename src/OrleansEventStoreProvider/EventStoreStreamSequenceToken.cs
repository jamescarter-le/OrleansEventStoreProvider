using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreStreamSequenceToken : EventSequenceToken
    {
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