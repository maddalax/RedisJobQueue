using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Cronos;
using RedisJobQueue.Models;
using RedisJobQueue.Utility;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace RedisJobQueue
{
    public class JobQueue : IJobQueue, IDisposable
    {
        private readonly ConnectionMultiplexer _connection;
        private ISubscriber _subscriber;
        private readonly Guid _id;
        private readonly string _schedulerJobName = "_job_scheduler";
        private readonly ConcurrentDictionary<string, Func<object, Task>> _listeners;
        private readonly ConcurrentDictionary<string, Func<Task>> _listenersNoArgs;
        private readonly RedLockFactory _lockFactory;
        private readonly JobQueueOptions _options = new JobQueueOptions();
        private readonly PollingExecutor _executor;

        public JobQueue(ConnectionMultiplexer connection)
        {
            _id = Guid.NewGuid();
            _connection = connection;
            _executor = new PollingExecutor(_options.PollRate);
            _listeners = new ConcurrentDictionary<string, Func<object, Task>>();
            _listenersNoArgs = new ConcurrentDictionary<string, Func<Task>>();
            _lockFactory = RedLockFactory.Create(new List<RedLockMultiplexer>
            {
                _connection
            });
        }
        
        public JobQueue(ConnectionMultiplexer connection, JobQueueOptions options) : this(connection)
        {
            _options = options;
        }
        
        private void Subscribe()
        {
            _subscriber = _connection.GetSubscriber();
            _subscriber.SubscribeAsync(_options.KeyPrefix, async (channel, value) =>
            {
                try
                {
                    var job = BsonSerializer.FromBson<Job>(value);
                    var key = $"{channel}_{job.Name}";
                    
                    if (!_listeners.ContainsKey(job.Name) && !_listenersNoArgs.ContainsKey(job.Name))
                    {
                        return;
                    }

                    using (var redLock = await _lockFactory.CreateLockAsync(key, _options.JobLockTimeout))
                    {
                        if (!redLock.IsAcquired)
                        {
                            return;
                        }

                        await OnJobStart(job);

                        try
                        {
                            if (_listeners.ContainsKey(job.Name))
                            {
                                await _listeners[job.Name](job.Parameters);
                            }
                            else
                            {
                                await _listenersNoArgs[job.Name]();
                            }
                        }
                        catch (Exception e)
                        {
                            await OnJobError(job, e);
                        }

                        if (job.Type == JobType.Scheduled)
                        {
                            await OnScheduledJobFinish(job);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            });
                
        }

        public void Start()
        {
            Subscribe();
            SubmitJobPoll();
            _executor.Start(CancellationToken.None);
            _connection.ConnectionRestored += (sender, args) => Subscribe();
        }

        private void SubmitJobPoll()
        {
           _executor.Submit(ExecuteScheduledJobs);
        }

        public async Task<long> Enqueue(string job, object args)
        {
            return await Enqueue(new Job
            {
                Name = job,
                Parameters = args
            });
        }
        
        public async Task<long> Enqueue(string job)
        {
            return await Enqueue(new Job { Name = job });
        }
        
        private async Task<long> Enqueue(Job job)
        {
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_jobs", job.Name);
            var serialized = BsonSerializer.ToBson(job);
            return await _connection.GetDatabase().PublishAsync(_options.KeyPrefix, serialized);
        }

        public async Task<bool> Schedule(string job, string cronExpression)
        {
            return await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_scheduled_jobs", BsonSerializer.ToBson(new Job
            {
                Name = job,
                Parameters = cronExpression
            }));
        }

        public void OnJob<T>(string job, Func<T, Task> callback)
        {
            var expression = new Func<object, Task>(o => callback((T) o));
            _listeners.TryAdd(job, expression);
        }
        
        public void OnJob(string job, Func<Task> callback)
        {
            Expression<Func<Task>> expression = () => callback();
            _listenersNoArgs.TryAdd(job, expression.Compile());
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _lockFactory?.Dispose();
        }

        private async Task ExecuteScheduledJobs()
        {
            var key = $"{_options.KeyPrefix}{_schedulerJobName}";
            
            using (var redLock = await _lockFactory.CreateLockAsync(key, _options.JobLockTimeout))
            {
                if (!redLock.IsAcquired)
                {
                    return;
                }

                var jobs = await _connection.GetDatabase().SetMembersAsync($"{_options.KeyPrefix}_scheduled_jobs");

                foreach (var value in jobs)
                {
                    var job = BsonSerializer.FromBson<Job>(value);
                    var expression = CronExpression.Parse((string) job.Parameters);
                    var occurence = await GetLastRun(job) ?? await GetLastCheck(job) ?? DateTime.UtcNow;
                    var nextUtc = expression.GetNextOccurrence(occurence);
                    if (!nextUtc.HasValue || nextUtc.Value > DateTime.UtcNow)
                    {
                        continue;
                    }
                    job.Type = JobType.Scheduled;
                    await Enqueue(job);
                }
            }
        }
        
        private async Task OnScheduledJobFinish(Job job)
        {
            var lastRun = $"{_options.KeyPrefix}_{job.Name}_last_run";
            var lastCheck = $"{_options.KeyPrefix}_{job.Name}_last_check";
            var ticks = DateTime.UtcNow.Ticks;
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_{job.Name}_runs", BsonSerializer.ToBson(new ExecutedJob
            {
                MachineId = _id,
                MachineName = Environment.MachineName,
                Name = job.Name,
                Timestamp = ticks
            }));
            await _connection.GetDatabase().StringSetAsync(lastRun, ticks);
            await _connection.GetDatabase().KeyDeleteAsync(lastCheck);
        }

        private async Task OnJobStart(Job job)
        {
            await _connection.GetDatabase().StringSetAsync($"{_options.KeyPrefix}_{job.Name}_details", BsonSerializer.ToBson(
                new JobExecutionDetail
                {
                    DateStarted = DateTime.UtcNow.Ticks,
                    Name = job.Name,
                    Parameters = job.Parameters,
                    Status = JobStatus.Running
                }));
        }
        
        private async Task OnJobError(Job job, Exception e)
        {
            var key = $"{_options.KeyPrefix}_{job.Name}_details";
            var current = _connection.GetDatabase().StringGet(key);
            if (!current.HasValue)
            {
                await _connection.GetDatabase().StringSetAsync(key, BsonSerializer.ToBson(
                    new JobExecutionDetail
                    {
                        DateStarted = DateTime.UtcNow.Ticks,
                        Name = job.Name,
                        Parameters = job.Parameters,
                        Status = JobStatus.Errored,
                        Exception = e
                    }));   
            }
            else
            {
                var details = BsonSerializer.FromBson<JobExecutionDetail>(current);
                details.Exception = e;
                details.Status = JobStatus.Errored;
                await _connection.GetDatabase().StringSetAsync(key, BsonSerializer.ToBson(
                    details));
            }
        }
        
        private async Task<DateTime?> GetLastRun(Job job)
        {
            var key = $"{_options.KeyPrefix}_{job.Name}_last_run";
            var ticks = await _connection.GetDatabase().StringGetAsync(key);
            if (!ticks.HasValue)
            {
                return null;
            }
            return new DateTime(long.Parse(ticks.ToString()), DateTimeKind.Utc);
        }
        
        private async Task<DateTime?> GetLastCheck(Job job)
        {
            var db = _connection.GetDatabase();
            var key = $"{_options.KeyPrefix}_{job.Name}_last_check";
            var now = DateTime.UtcNow;
            var ticks = await db.StringGetAsync(key);
            if (ticks.HasValue)
            {
                return new DateTime(long.Parse(ticks.ToString()), DateTimeKind.Utc);
            }
            await db.StringSetAsync(key, now.Ticks);
            return now;
        }
    }
}