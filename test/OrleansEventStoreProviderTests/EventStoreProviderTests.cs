using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI.Embedded;
using EventStore.Core;
using Orleans;
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

        private static ClusterVNode EventStoreNode;
        private const bool EnableEmbeddedEventStore = false;

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

            StartEmbeddedEventStore();
        }

        public EventStoreProviderTests() : base(m_SiloOptions, m_ClientOptions)
        {
        }

        private static void StartEmbeddedEventStore()
        {
            if (EnableEmbeddedEventStore)
            {
                var nodeBuilder = EmbeddedVNodeBuilder.AsSingleNode().OnDefaultEndpoints().RunInMemory();
                nodeBuilder.WithStatsPeriod(TimeSpan.FromSeconds(1));
                EventStoreNode = nodeBuilder.Build();
                EventStoreNode.StartAndWaitUntilReady().Wait();
            }
        }

        [Fact]
        public async Task CanSubscribe()
        {
            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain<object>>(Guid.Empty);
            await subscriber.SubscribeTo(Guid.Empty, "$stats-127.0.0.1:2113", ProviderName);
            await TestingUtils.WaitUntilAsync(lastTry => subscriber.HasReceivedMessage(), TimeSpan.FromSeconds(30));

            Assert.True(await subscriber.HasReceivedMessage());
        }

        [Fact]
        public async Task CanDeserializeJsonMessage()
        {
            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain<Dictionary<string, string>>>(Guid.Empty);
            await subscriber.SubscribeTo(Guid.Empty, "$stats-127.0.0.1:2113", ProviderName);
            await TestingUtils.WaitUntilAsync(lastTry => subscriber.HasReceivedMessage(), TimeSpan.FromSeconds(30));

            var msg = await subscriber.ReceivedMessage();
            Assert.IsAssignableFrom<Dictionary<string, string>>(msg);
            Assert.True(msg.ContainsKey("proc-id"));
        }

        [Fact]
        public async Task CanReadFromStartOfStream()
        {
            var testStream = $"TestStream-{Guid.NewGuid()}";

            var provider = GrainClient.GetStreamProvider(ProviderName);
            var stream = provider.GetStream<int>(Guid.Empty, testStream);
            await stream.OnNextAsync(1);
            await stream.OnNextAsync(2);
            await stream.OnNextAsync(3);
            await stream.OnNextAsync(4);
            await stream.OnNextAsync(5);

            var subscriber = GrainFactory.GetGrain<IStreamSubscriberGrain<int>>(Guid.Empty);
            await subscriber.SubscribeFrom(Guid.Empty, testStream, ProviderName, -1);
            await TestingUtils.WaitUntilAsync(async lastTry => await subscriber.ReceivedCount() == 5, TimeSpan.FromSeconds(30));

            Assert.Equal(5, await subscriber.ReceivedCount());
        }

        public void Dispose()
        {
            if (EnableEmbeddedEventStore)
            {
                EventStoreNode.StopNonblocking(false, true);
            }
        }
    }
}
