using System;
using System.Threading.Tasks;

namespace RedisJobQueue
{
    public interface IJobQueue
    {
        void Start();
        Task Enqueue(string job, object args);
        Task Enqueue(string job);
        Task<bool> Schedule(string job, string cronExpression);
        void OnJob<T>(string job, Func<T, Task> callback);
        void OnJob(string job, Func<Task> callback);
    }
}