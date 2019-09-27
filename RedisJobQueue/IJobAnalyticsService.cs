using System.Collections.Generic;
using System.Threading.Tasks;
using RedisJobQueue.Models;

namespace RedisJobQueue
{
    public interface IJobAnalyticsService
    {
        Task<HashSet<string>> GetJobs();
        Task<IEnumerable<ExecutedJob>> GetRuns(string job);
        Task<long> GetEnqueuedCount();
    }
}