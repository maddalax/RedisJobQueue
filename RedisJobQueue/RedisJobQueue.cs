using RedisJobQueue.Models;
using StackExchange.Redis;

namespace RedisJobQueue
{
    public class RedisJobQueue
    {
        public RedisJobQueue(ConnectionMultiplexer connection, JobQueueOptions options)
        {
            Options = options;
            Queue = new JobQueue(connection, Options);
            Analytics = new JobAnalyticsService(connection, Options);
        }
        
        public JobQueueOptions Options { get; }
        public IJobQueue Queue { get; }
        public IJobAnalyticsService Analytics { get; }
    }
}