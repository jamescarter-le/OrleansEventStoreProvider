using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Orleans;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using OrleansEventStoreProvider;
using Xunit;

namespace OrleansEventStoreProviderTests
{
    public interface IStreamSubscriberGrain : IGrainWithGuidKey
    {
        Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName);
        Task<bool> HasReceivedMessage();
    }

    public class StreamSubscriberGrain : Grain, IStreamSubscriberGrain
    {
        private bool m_ReceivedMessage;

        public async Task SubscribeTo(Guid streamGuid, string streamNamespace, string providerName)
        {
            var provider = this.GetStreamProvider(providerName);
            var stream = provider.GetStream<int>(streamGuid, streamNamespace);
            await stream.SubscribeAsync(async (i, s) => m_ReceivedMessage = true);
        }

        public Task<bool> HasReceivedMessage() => Task.FromResult(m_ReceivedMessage);
    }

    public class EventStoreProviderTests : TestingSiloHost
    {
        private const string ProviderName = "EventStoreStreamProvider";
        // Default settings of EventStore
        private const string ConnectionString = "ConnectTo=tcp://admin:changeit@localhost:1113";

        private static readonly TestingSiloOptions m_SiloOptions;
        private static readonly TestingClientOptions m_ClientOptions;

        static EventStoreProviderTests()
        {
            m_SiloOptions = new TestingSiloOptions
            {
                StartPrimary = true,
                StartSecondary = false,
                StartClient = true,
                AdjustConfig = (config) =>
                {
                    config.Globals.RegisterStreamProvider<EventStoreStreamProvider>(ProviderName,
                        new Dictionary<string, string>()
                        {
                            {"ConnectionString", ConnectionString}
                        });
                    config.Globals.RegisterStorageProvider<MemoryStorage>("PubSubStore");
                }
            };

            m_ClientOptions = new TestingClientOptions
            {
                AdjustConfig = (config) =>
                {
                    config.RegisterStreamProvider<EventStoreStreamProvider>(ProviderName,
                        new Dictionary<string, string>()
                        {
                            {"ConnectionString", ConnectionString}
                        });
                }
            };
        }

        public EventStoreProviderTests() : base(m_SiloOptions, m_ClientOptions)
        {
        }

        [Fact]
        public async Task CanBoot()
        {
        }

        [Fact]
        public async Task CanSubscribe()
        {
            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain>(Guid.Empty);
            await subscriber.SubscribeTo(Guid.Empty, "$stats-127.0.0.1:2113", ProviderName);
            await TestingUtils.WaitUntilAsync((x) => subscriber.HasReceivedMessage(), TimeSpan.FromSeconds(30));
        }
    }
}
