using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RedisJobQueue.Models;
using RedisJobQueue.Utility;
using StackExchange.Redis;

namespace RedisJobQueue
{
    public class JobAnalyticsService : IJobAnalyticsService
    {
        private readonly ConnectionMultiplexer _connection;
        private readonly JobQueueOptions _options;

        public JobAnalyticsService(ConnectionMultiplexer connection, JobQueueOptions options)
        {
            _connection = connection;
            _options = options;
        }
        
        public async Task<HashSet<string>> GetJobs()
        {
            var members = await _connection.GetDatabase().SetMembersAsync($"{_options.KeyPrefix}_jobs");
            return members.Select(w => w.ToString()).ToHashSet();
        }
        
        public async Task<IEnumerable<ExecutedJob>> GetRuns(string job)
        {
            var db = _connection.GetDatabase();
            var ids = await db.SetMembersAsync($"{_options.KeyPrefix}_{job}_runs");
            var keys = ids.Select(w => (RedisKey) $"{_options.KeyPrefix}_{w.ToString()}_run").ToArray();
            var values = await db.StringGetAsync(keys);
            return values
                .Where(w => w.HasValue)
                .Select(w => BsonSerializer.FromBson<ExecutedJob>(w));
        }
    }
}