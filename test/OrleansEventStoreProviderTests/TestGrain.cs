using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers.Streams.EventStore;
using Orleans.Streams;

namespace OrleansEventStoreProviderTests
{
    public interface IStreamSubscriberGrain<T> : IGrainWithGuidKey
    {
        Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName);
        Task SubscribeFrom(Guid streamGuid, string streamNamespace, string providerName, int fromIndex);
        Task<bool> HasReceivedMessage();
        Task<T> ReceivedMessage();
        Task<int> ReceivedCount();
    }

    public class StreamSubscriberGrain<T> : Grain, IStreamSubscriberGrain<T>
    {
        private int m_ReceivedCount;
        private T m_ReceivedMessage;

        public async Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName)
        {
            var provider = this.GetStreamProvider(providerName);
            var stream = provider.GetStream<T>(streamGuid, streamNamespace);
            await stream.SubscribeAsync(async (msg, s) =>
            {
                m_ReceivedMessage = msg;
                m_ReceivedCount += 1;
            });
        }

        public async Task SubscribeFrom(Guid streamGuid, string streamNamespace, string providerName, int index)
        {
            var provider = this.GetStreamProvider(providerName);
            var stream = provider.GetStream<T>(streamGuid, streamNamespace);
            await stream.SubscribeAsync(async (msg, s) =>
            {
                m_ReceivedMessage = msg;
                m_ReceivedCount += 1;
            }, new EventStoreStreamSequenceToken(index));
        }

        public Task<bool> HasReceivedMessage() => Task.FromResult(m_ReceivedMessage != null);
        public Task<T> ReceivedMessage() => Task.FromResult(m_ReceivedMessage);
        public Task<int> ReceivedCount() => Task.FromResult(m_ReceivedCount);

    }
}
