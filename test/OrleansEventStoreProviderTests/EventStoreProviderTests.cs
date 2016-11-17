using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;
using OrleansEventStoreProvider;
using Xunit;

namespace OrleansEventStoreProviderTests
{
    public class EventStoreProviderTests : TestingSiloHost
    {
        private const string ProviderName = "EventStoreStreamProvider";
        private const string ConnectionString = "ConnectTo=tcp://localhost:1113";

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
            var stream = GrainClient.GetStreamProvider(ProviderName).GetStream<int>(Guid.Empty, "$stats-127.0.0.1:2113");
            TaskCompletionSource<int> completion = new TaskCompletionSource<int>();
            await stream.OnNextAsync(100);
            await stream.SubscribeAsync(async (val, sequence) =>
            {
                completion.SetResult(1);
            });

            await completion.Task;
        }
    }
}
