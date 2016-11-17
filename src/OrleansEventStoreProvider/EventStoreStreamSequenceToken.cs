using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreStreamSequenceToken : StreamSequenceToken
    {
        public int EventNumber { get; }

        public EventStoreStreamSequenceToken(int eventNumber)
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