using System;
using System.Threading.Tasks;
using RedisJobQueue.Models;

namespace RedisJobQueue
{
    public interface IJobQueue
    {
        void Start();
        Task Enqueue(string job, object args);
        Task Enqueue(string job);
        Task<bool> Schedule(string job, string cronExpression);
        Task OnJob<T>(string job, Func<T, Task> callback);
        Task OnJob(string job, Func<Task> callback);
        Task OnJob(string job, JobOptions options, Func<Task> callback);
    }
}