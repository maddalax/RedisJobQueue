using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RedisJobQueue.Models;

namespace RedisJobQueue
{
    public interface IJobQueue
    {
        void Start();
        Task Enqueue(string job, object args);
        Task EnqueueMany(IEnumerable<string> job);
        Task Enqueue(string job);
        Task Schedule(string job, string cronExpression);
        Task Interval(string job, TimeSpan span);
        Task OnJob<T>(string job, Func<T, Task> callback);
        Task OnJob(string job, Func<Task> callback);
        void OnScheduledJob(string job, Func<Task> callback);
        Task OnJob(string job, JobOptions options, Func<Task> callback);
    }
}