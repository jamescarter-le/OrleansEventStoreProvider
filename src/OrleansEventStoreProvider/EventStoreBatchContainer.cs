using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using EventStore.ClientAPI;
using MsgPack;
using MsgPack.Serialization;
using Newtonsoft.Json;
using Orleans.Streams;

namespace OrleansEventStoreProvider
{
    public class EventStoreBatchContainer : IBatchContainer
    {
        public Guid StreamGuid { get; }
        public string StreamNamespace { get; }
        public StreamSequenceToken SequenceToken { get; }
        public ResolvedEvent ResolvedEvent { get; }

        public EventStoreBatchContainer(Guid streamGuid, string streamNamespace, StreamSequenceToken sequenceToken, ResolvedEvent resolvedEvent)
        {
            ResolvedEvent = resolvedEvent;
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            SequenceToken = sequenceToken;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            yield return new Tuple<T, StreamSequenceToken>(DeserializeData<T>(), SequenceToken);
        }

        private T DeserializeData<T>()
        {
            string encoding = "json";
            
            // If the Event is marked as IsJson we assume the Data is Json.
            if (!ResolvedEvent.Event.IsJson)
            {
                var metadata = JsonDeserialize<Dictionary<string, object>>(ResolvedEvent.Event.Metadata);
                if(!metadata.ContainsKey("encoding"))
                    throw new SerializationException("The metadata should always be JSON, and contain a property named \"encoding\" which specifies the encoding of the Data.");
                encoding = metadata["encoding"] as string;
            }

            return Deserialize<T>(encoding, ResolvedEvent.Event.Data);
        }

        private T Deserialize<T>(string encoding, byte[] data)
        {
            switch (encoding.ToLowerInvariant())
            {
                case "json":
                    return JsonDeserialize<T>(data);
                case "msgpack":
                    return MsgPackDeserialize<T>(data);
                default:
                    throw new SerializationException($"We do not support encoding specified: {encoding}");
            }
        }

        private T JsonDeserialize<T>(byte[] data)
        {
            // Can we access a pooled JsonSerializer?
            using (var ms = new MemoryStream(data))
            using (var sr = new StreamReader(ms))
            using (var jr = new JsonTextReader(sr))
            {
                return JsonSerializer.CreateDefault().Deserialize<T>(jr);
            }
        }

        private T MsgPackDeserialize<T>(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                return SerializationContext.Default.GetSerializer<T>().Unpack(ms);
            }
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