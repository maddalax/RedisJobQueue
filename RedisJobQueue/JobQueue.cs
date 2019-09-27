using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Cronos;
using RedisJobQueue.Models;
using RedisJobQueue.Models.Exceptions;
using RedisJobQueue.Utility;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace RedisJobQueue
{
    public class JobQueue : IJobQueue, IDisposable
    {
        private int count;
        private readonly ConnectionMultiplexer _connection;
        private readonly Guid _id;
        private readonly string _schedulerJobName = "_job_scheduler";
        private readonly ConcurrentDictionary<string, Func<object, Task>> _listeners;
        private readonly ConcurrentDictionary<string, Func<Task>> _listenersNoArgs;
        private readonly RedLockFactory _lockFactory;
        private readonly ISubscriber _subscriber;
        private readonly JobQueueOptions _options = new JobQueueOptions();
        private readonly PollingExecutor _executor;
        private readonly SemaphoreSlim _semaphore;
        private readonly SemaphoreSlim _pollingSemaphore;


        public JobQueue(ConnectionMultiplexer connection)
        {
            _id = Guid.NewGuid();
            _connection = connection;
            _subscriber = connection.GetSubscriber();
            _executor = new PollingExecutor(_options.PollRate);
            _listeners = new ConcurrentDictionary<string, Func<object, Task>>();
            _listenersNoArgs = new ConcurrentDictionary<string, Func<Task>>();
            _semaphore = new SemaphoreSlim(_options.MaxConcurrentJobs, _options.MaxConcurrentJobs);
            _pollingSemaphore = new SemaphoreSlim(1, 1);
            _lockFactory = RedLockFactory.Create(new List<RedLockMultiplexer>
            {
                _connection
            });
        }

        public JobQueue(ConnectionMultiplexer connection, JobQueueOptions options) : this(connection)
        {
            _options = options;
        }

        public void Start()
        {
            _subscriber.SubscribeAsync($"{_options.KeyPrefix}_on_enqueue", async delegate { await ExecutePoll(); });
            SubmitJobPoll();
            _executor.Start(CancellationToken.None);
        }

        public async Task Enqueue(string job, object args, JobOptions options)
        {
            await Enqueue(new Job {Name = job, Parameters = args, Options = options});
        }

        private void SubmitJobPoll()
        {
            _executor.Submit(ExecutePoll);
            _executor.Submit(ExecuteScheduledJobs);
            _executor.Submit(ExecuteJobRetry);
        }

        public async Task Enqueue(string job, object args)
        {
            var exists = await GetJobOrThrow(job);
            exists.Parameters = args;
            await Enqueue(exists);
        }
        
        public async Task Enqueue(string job)
        {
            var exists = await GetJobOrThrow(job);
            await Enqueue(exists);
        }

        private async Task Enqueue(Job job)
        {
            job.RunId = Guid.NewGuid();
            await SaveJobRun(job, JobStatus.Enqueued);
            await DoEnqueue(job);
        }

        private async Task Enqueue(ExecutedJob run)
        {
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_jobs", run.Name);
            run.Status = JobStatus.Enqueued;
            await SaveJobRun(run);
            await DoEnqueue(run);
        }

        private async Task DoEnqueue(object value)
        {
            var serialized = BsonSerializer.ToBson(value);
            await _connection.GetDatabase().ListRightPushAsync($"{_options.KeyPrefix}_enqueued", serialized,
                When.Always, CommandFlags.FireAndForget);
            await _connection.GetDatabase().PublishAsync($"{_options.KeyPrefix}_on_enqueue", "");
        }

        public async Task<bool> Schedule(string job, string cronExpression)
        {
            var j = new Job
            {
                Name = job,
                Parameters = cronExpression
            };
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_jobs", j.Name);
            return await _connection.GetDatabase()
                .SetAddAsync($"{_options.KeyPrefix}_scheduled_jobs", BsonSerializer.ToBson(j));
        }

        public async Task OnJob<T>(string job, Func<T, Task> callback)
        {
            await UpsertJob(job, new JobOptions());
            var expression = new Func<object, Task>(o => callback((T) o));
            _listeners.TryAdd(job, expression);
        }

        public async Task OnJob(string job, Func<Task> callback)
        {
            await OnJob(job, new JobOptions(), callback);
        }
        
        public async Task OnJob(string job, JobOptions options, Func<Task> callback)
        {
            await UpsertJob(job, options);
            Expression<Func<Task>> expression = () => callback();
            _listenersNoArgs.TryAdd(job, expression.Compile());
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _lockFactory?.Dispose();
        }

        private async Task ExecutePoll()
        {
            try
            {
                await _pollingSemaphore.WaitAsync();
                var tasks = new List<Task>();

                for (var i = 0; i < _options.MaxConcurrentJobs; i++)
                {
                    tasks.Add(Task.Run(ExecuteQueue));
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                _pollingSemaphore.Release();
            }
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

        private async Task ExecuteJobRetry()
        {
            var key = $"{_options.KeyPrefix}_job_retries";
            var runIds = await _connection.GetDatabase().SetMembersAsync(key);
            foreach (var value in runIds)
            {
                var run = await GetRun(Guid.Parse(value.ToString()));

                if (run == null || run.Status != JobStatus.Retrying)
                {
                    continue;
                }

                if (DateTime.UtcNow.Ticks < run.NextRetry)
                {
                    continue;
                }

                await Enqueue(run);
            }
        }

        private async Task OnImmediateJobFinish(Job job)
        {
        }

        private async Task OnScheduledJobFinish(Job job)
        {
            var lastCheck = $"{_options.KeyPrefix}_{job.Name}_last_check";
            await _connection.GetDatabase().KeyDeleteAsync(lastCheck);
        }

        private async Task SaveJobRun(Job job, JobStatus status)
        {
            var run = await GetRun(job);
            var execution = new ExecutedJob(job, status, _id);
            if (run != null)
            {
                execution.Retries = run.Retries;
                execution.NextRetry = run.NextRetry;
            }

            await SaveJobRun(execution);
        }

        private async Task SaveJobRun(ExecutedJob job)
        {
            job.Timestamp = DateTime.UtcNow.Ticks;
            var lastRun = $"{_options.KeyPrefix}_{job.Name}_last_run";
            await _connection.GetDatabase()
                .StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_run", BsonSerializer.ToBson(job));
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_{job.Name}_runs", job.RunId.ToString());
            await _connection.GetDatabase().StringSetAsync(lastRun, job.Timestamp);
        }

        private async Task<ExecutedJob> GetRun(Job job)
        {
            return await GetRun(job.RunId);
        }

        private async Task<ExecutedJob> GetRun(Guid runId)
        {
            var key = $"{_options.KeyPrefix}_{runId}_run";
            var run = await _connection.GetDatabase().StringGetAsync(key);
            return run.HasValue ? BsonSerializer.FromBson<ExecutedJob>(run) : null;
        }

        private async Task UpsertJob(string name, JobOptions options)
        {
            using (var redLock =
                await _lockFactory.CreateLockAsync($"{_options.KeyPrefix}_{name}_create", TimeSpan.FromSeconds(5)))
            {
                if (!redLock.IsAcquired)
                {
                    return;
                }
                var db = _connection.GetDatabase();
                var transaction = db.CreateTransaction();
                var job = new Job
                {
                    Name = name,
                    Options = options
                };
                // Do not await because redis transactions will not if you await the set add.
#pragma warning disable 4014
                transaction.StringSetAsync($"{_options.KeyPrefix}_job_{name}_meta", BsonSerializer.ToBson(job));
#pragma warning restore 4014
#pragma warning disable 4014
                transaction.SetAddAsync($"{_options.KeyPrefix}_jobs", name);
#pragma warning restore 4014
                await transaction.ExecuteAsync();
            }
        }

        private async Task<Job> GetJob(string name)
        {
            var data = await _connection.GetDatabase().StringGetAsync($"{_options.KeyPrefix}_job_{name}_meta");
            return !data.HasValue ? null : BsonSerializer.FromBson<Job>(data);
        }
        
        private async Task<Job> GetJobOrThrow(string name)
        {
            var job = await GetJob(name);
            
            if (job == null)
            {
                throw new Exception($"Failed to retrieved job {name}. Please add job using .OnJob() method.");
            }

            return job;
        }
        
        private async Task UpsertJob(string name)
        {
            await UpsertJob(name, new JobOptions());
        }
        
        private async Task OnJobStart(Job job)
        {
            await SaveJobRun(job, JobStatus.Running);
            await _connection.GetDatabase().StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_details",
                BsonSerializer.ToBson(
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
            var run = await GetRun(job) ?? new ExecutedJob(job, JobStatus.Retrying, _id);
            run.Exception = e;
            if (run.Retries < _options.MaxRetries && run.Status != JobStatus.Errored)
            {
                await RetryJob(job, run);
            }
            else
            {
                run.Status = JobStatus.Errored;
                await SaveJobRun(run);
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

        private async Task RetryJob(Job job, ExecutedJob run)
        {
            run.Retries++;
            run.Status = JobStatus.Retrying;
            run.NextRetry = CalculateNextRetry(run);
            await SaveJobRun(run);
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_job_retries", job.RunId.ToString());
        }


        private long CalculateNextRetry(ExecutedJob run)
        {
            return (run.Retries * _options.RetryBackOff).Ticks;
        }

        private async Task ExecuteQueue()
        {
            await _semaphore.WaitAsync();

            Job job = null;
            try
            {
                var db = _connection.GetDatabase();
                var value = await db.ListLeftPopAsync($"{_options.KeyPrefix}_enqueued");

                if (!value.HasValue)
                {
                    return;
                }

                job = BsonSerializer.FromBson<Job>(value);
                var key = $"{_options.KeyPrefix}_{job.Name}_lock";

                if (job.Options != null && !job.Options.AllowConcurrentExecution)
                {
                    using (var jobLock = await _lockFactory.CreateLockAsync(key, TimeSpan.FromSeconds(30)))
                    {
                        if (!jobLock.IsAcquired)
                        {
                            await db.ListLeftPushAsync($"{_options.KeyPrefix}_enqueued", value);
                            return;
                        }

                        await ExecuteJob(job);
                    }
                }
                else
                {
                    await ExecuteJob(job);
                }
            }
            catch (Exception e)
            {
                if (_options.OnQueueError != null)
                {
                    await _options.OnQueueError(e);
                }

                if (job != null)
                {
                    await OnJobError(job, e);
                }

                if (_options.OnQueueError == null)
                {
                    throw;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        private async Task ExecuteJob(Job job)
        {
            if (!_listeners.ContainsKey(job.Name) && !_listenersNoArgs.ContainsKey(job.Name))
            {
                await OnJobError(job, new NoHandlerFoundException(job));
                return;
            }

            count++;
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
                return;
            }

            await SaveJobRun(job, JobStatus.Success);
            Console.WriteLine(count);

            switch (job.Type)
            {
                case JobType.Immediate:
                    await OnImmediateJobFinish(job);
                    break;
                case JobType.Scheduled:
                    await OnScheduledJobFinish(job);
                    break;
            }
        }
    }
}