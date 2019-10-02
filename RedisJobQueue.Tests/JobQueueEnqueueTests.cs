using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RedisJobQueue.Models;
using StackExchange.Redis;
using Xunit;

namespace RedisJobQueue.Tests
{
    public class JobQueueEnqueueTests : IDisposable
    {
        private readonly ConnectionMultiplexer _multiplexer = ConnectionMultiplexer.Connect("localhost:6379,abortConnect=false,allowAdmin=true");

        private readonly JobQueueOptions _options = new JobQueueOptions
        {
            PollRate = TimeSpan.FromSeconds(1),
            Namespace = Guid.NewGuid().ToString()
        };

        private JobQueue NewQueue()
        {
            return new JobQueue(_multiplexer, _options);
        }

        [Fact]
        public async Task Should_Enqueue_Job_With_Parameter()
        {   var autoReset = new AutoResetEvent(false);
            var passed = string.Empty;
            var queue = NewQueue();
            await queue.OnJob("test_job_with_params", (string value) =>
            {
                autoReset.Set();
                passed = value;
                return Task.CompletedTask;
            });
            queue.Start();
            await queue.Enqueue("test_job_with_params", "parameter");
            Assert.True(autoReset.WaitOne());
            Assert.Equal("parameter", passed);
        }
        
        [Fact]
        public async Task Should_Enqueue_Job_Without_Parameter()
        {   var autoReset = new AutoResetEvent(false);
            var queue = NewQueue();
            await queue.OnJob("test_job", () =>
            {
                autoReset.Set();
                return Task.CompletedTask;
            });
            queue.Start();
            await queue.Enqueue("test_job");
            Assert.True(autoReset.WaitOne());
        }

        [Fact]
        public async Task Should_Enqueue_On_Interval()
        {
            var autoReset = new AutoResetEvent(false);
            var queue = NewQueue();
            queue.OnScheduledJob("interval_test_job", () =>
            {
                autoReset.Set();
                return Task.CompletedTask;
            });
            queue.Start();
            await queue.Interval("interval_test_job", TimeSpan.FromSeconds(1));
            Assert.True(autoReset.WaitOne());
        }

        public void Dispose()
        {
            foreach (var ep in _multiplexer.GetEndPoints())
            {
                var server = _multiplexer.GetServer(ep);
                var keys = server.Keys(pattern:  $"{_options.KeyPrefix}*").ToArray();
                _multiplexer.GetDatabase().KeyDeleteAsync(keys);
            }
        }
    }
}