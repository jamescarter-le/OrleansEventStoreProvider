using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace OrleansEventStoreProviderTests
{
    public interface IStreamSubscriberGrain<T> : IGrainWithGuidKey
    {
        Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName);
        Task<bool> HasReceivedMessage();
        Task<T> ReceivedMessage();
    }

    public class StreamSubscriberGrain<T> : Grain, IStreamSubscriberGrain<T>
    {
        private T m_ReceivedMessage;

        public async Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName)
        {
            var provider = this.GetStreamProvider(providerName);
            var stream = provider.GetStream<T>(streamGuid, streamNamespace);
            await stream.SubscribeAsync(async (msg, s) => { m_ReceivedMessage = msg; });
        }

        public Task<bool> HasReceivedMessage() => Task.FromResult(m_ReceivedMessage != null);
        public Task<T> ReceivedMessage() => Task.FromResult(m_ReceivedMessage);
    }
}
