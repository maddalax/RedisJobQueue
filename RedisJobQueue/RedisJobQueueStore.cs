using RedisJobQueue.Models;
using StackExchange.Redis;

namespace RedisJobQueue
{
    public class RedisJobQueueStore
    {
        public RedisJobQueueStore(ConnectionMultiplexer connection, JobQueueOptions options)
        {
            Options = options;
            Queue = new RedisJobQueue(connection, Options);
            Analytics = new JobAnalyticsService(connection, Options);
        }
        
        public JobQueueOptions Options { get; }
        public IRedisJobQueue Queue { get; }
        public IJobAnalyticsService Analytics { get; }
    }
}