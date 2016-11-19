using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI.Embedded;
using EventStore.Core;
using Orleans.Providers.Streams.EventStore;
using Orleans.Storage;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using Xunit;

namespace OrleansEventStoreProviderTests
{
    public class EventStoreProviderTests : TestingSiloHost, IDisposable
    {
        private const string ProviderName = "EventStoreStreamProvider";
        // Default Endpoints of EventStore
        private const string ConnectionString = "ConnectTo=tcp://admin:changeit@localhost:1113";

        private static readonly TestingSiloOptions m_SiloOptions;
        private static readonly TestingClientOptions m_ClientOptions;
        private ClusterVNode m_EventStoreNode;

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
            StartEmbeddedEventStore();
        }

        private void StartEmbeddedEventStore()
        {
            var nodeBuilder = EmbeddedVNodeBuilder.AsSingleNode().OnDefaultEndpoints().RunInMemory();
            nodeBuilder.WithStatsPeriod(TimeSpan.FromSeconds(1));
            m_EventStoreNode = nodeBuilder.Build();
            m_EventStoreNode.StartAndWaitUntilReady().Wait();
        }

        [Fact]
        public async Task CanSubscribe()
        {
            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain<object>>(Guid.Empty);
            await subscriber.SubscribeTo(Guid.Empty, "$stats-127.0.0.1:2113", ProviderName);
            await TestingUtils.WaitUntilAsync(lastTry => subscriber.HasReceivedMessage(), TimeSpan.FromSeconds(5));
        }

        [Fact]
        public async Task CanDeserializeJsonMessage()
        {
            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain<Dictionary<string, string>>>(Guid.Empty);
            await subscriber.SubscribeTo(Guid.Empty, "$stats-127.0.0.1:2113", ProviderName);
            await TestingUtils.WaitUntilAsync(lastTry => subscriber.HasReceivedMessage(), TimeSpan.FromSeconds(5));

            var msg = await subscriber.ReceivedMessage();
            Assert.IsAssignableFrom<Dictionary<string, string>>(msg);
            Assert.True(msg.ContainsKey("proc-id"));
        }

        public void Dispose()
        {
            m_EventStoreNode.StopNonblocking(true, true);
        }
    }
}
