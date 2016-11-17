using System;
using System.Collections.Generic;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreBatchContainer : IBatchContainer
    {
        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken SequenceToken { get; }
        private readonly byte[] m_Message;

        public EventStoreBatchContainer(Guid streamGuid, string streamNamespace, StreamSequenceToken sequenceToken, byte[] message)
        {
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            SequenceToken = sequenceToken;
            m_Message = message;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            yield return new Tuple<T, StreamSequenceToken>(default(T), SequenceToken);
        }

        public bool ImportRequestContext()
        {
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            return true;
        }
    }
}