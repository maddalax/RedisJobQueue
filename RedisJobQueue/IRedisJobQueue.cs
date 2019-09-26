using System;
using System.Threading.Tasks;

namespace RedisJobQueue
{
    public interface IRedisJobQueue
    {
        void Start();
        Task<long> Enqueue(string job, object args);
        Task<long> Enqueue(string job);
        
        Task<bool> Schedule(string job, string cronExpression);
        void OnJob<T>(string job, Func<T, Task> callback);
        void OnJob(string job, Func<Task> callback);
    }
}