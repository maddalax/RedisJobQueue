using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
            _semaphore = new SemaphoreSlim(_options.MaxConcurrentJobs);
            _pollingSemaphore = new SemaphoreSlim(1, 1);
            _lockFactory = RedLockFactory.Create(new List<RedLockMultiplexer>
            {
                _connection
            });
        }

        public JobQueue(ConnectionMultiplexer connection, JobQueueOptions options) : this(connection)
        {
            _options = options;
            _executor = new PollingExecutor(_options.PollRate);
            _semaphore = new SemaphoreSlim(_options.MaxConcurrentJobs);
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
            _executor.Submit(TrimJobRun);
            _executor.Submit(ExecuteScheduledJobs);
            _executor.Submit(ExecuteJobRetry);
        }

        public async Task Enqueue(string job, object args)
        {
            var exists = await GetJobOrThrow(job);
            exists.Parameters = args;
            await Enqueue(exists);
        }

        public async Task EnqueueMany(IEnumerable<string> jobs)
        {
            var fullJobs = await GetJobsOrThrow(jobs.ToList());
            await EnqueueMany(fullJobs);
        }

        public async Task Enqueue(string job)
        {
            var exists = await GetJobOrThrow(job);
            await Enqueue(exists);
        }

        private async Task Enqueue(Job job)
        {
            await EnqueueMany(new[] {job});
        }

        private async Task Enqueue(ExecutedJob run)
        {
            await EnqueueMany(new[] {run});
        }

        public async Task EnqueueMany(IEnumerable<Job> jobs)
        {
            var transaction = _connection.GetDatabase().CreateTransaction();
            foreach (var job in jobs)
            {
                job.RunId = Guid.NewGuid();
                await SaveJobRun(job, JobStatus.Enqueued, transaction);
                DoEnqueue(job.RunId, transaction);
                transaction.SetRemoveAsync($"{_options.KeyPrefix}_job_retries", job.RunId.ToString());
            }

            await transaction.ExecuteAsync(CommandFlags.FireAndForget);
        }

        private async Task EnqueueMany(IEnumerable<ExecutedJob> runs)
        {
            var transaction = _connection.GetDatabase().CreateTransaction();

            foreach (var run in runs)
            {
                transaction.SetAddAsync($"{_options.KeyPrefix}_jobs", run.Name);
                run.Status = JobStatus.Enqueued;
                await SaveJobRun(run, transaction);
                DoEnqueue(run.RunId, transaction);
                transaction.SetRemoveAsync($"{_options.KeyPrefix}_job_retries", run.RunId.ToString());
            }

            await transaction.ExecuteAsync(CommandFlags.FireAndForget);
        }

        private void DoEnqueue(Guid runId, ITransaction transaction)
        {
            transaction.ListLeftPushAsync($"{_options.KeyPrefix}_enqueued", runId.ToString(),
                When.Always, CommandFlags.FireAndForget);
            transaction.PublishAsync($"{_options.KeyPrefix}_on_enqueue", "");
        }

        public async Task Schedule(string job, string cronExpression)
        {
            var j = new Job
            {
                Name = job,
                Parameters = cronExpression
            };
            var transaction = _connection.GetDatabase().CreateTransaction();
            transaction.SetAddAsync($"{_options.KeyPrefix}_scheduled_jobs", job, CommandFlags.FireAndForget);
            await UpsertJob(j, transaction);
        }
        
        public async Task Interval(string job, TimeSpan span)
        {
            var j = new Job
            {
                Name = job,
                Parameters = span.TotalMilliseconds.ToString()
            };
            var transaction = _connection.GetDatabase().CreateTransaction();
            transaction.SetAddAsync($"{_options.KeyPrefix}_scheduled_jobs", job, CommandFlags.FireAndForget);
            await UpsertJob(j, transaction);
        }

        public async Task OnJob<T>(string job, Func<T, Task> callback)
        {
            await UpsertJob(job, new JobOptions());
            var expression = new Func<object, Task>(o => callback(o == null ? default : (T) o));
            _listeners.TryAdd(job, expression);
        }

        public async Task OnJob(string job, Func<Task> callback)
        {
            await OnJob(job, new JobOptions(), callback);
        }

        public void OnScheduledJob(string job, Func<Task> callback)
        {
            Expression<Func<Task>> expression = () => callback();
            _listenersNoArgs.TryAdd(job, expression.Compile());
        }

        public async Task OnJob(string job, JobOptions options, Func<Task> callback)
        {
            await UpsertJob(job, options);
            var expression = new Func<object, Task>(o => callback());
            _listeners.TryAdd(job, expression);
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

                var jobNames = await _connection.GetDatabase().SetMembersAsync($"{_options.KeyPrefix}_scheduled_jobs");
                var names = jobNames.Select(w => w.ToString()).ToHashSet();

                var jobs = await GetJobs(names);
                var jobDict = jobs.ToDictionary(w => w.Name, w => w);
                var runs = await GetLastRuns(jobs);

                var jobsNotRun = names.Where(w => !runs.ContainsKey(w));

                if (jobsNotRun.Any())
                {
                    var jobsLastCheck = await GetLastChecks(jobsNotRun);
                    foreach (var (job, value) in jobsLastCheck)
                    {
                        runs.Add(job, value);
                    }
                }

                var toExecute = new List<Job>();
                foreach (var pair in runs)
                {
                    var (name, value) = pair;
                    var job = jobDict[name];
                    DateTime? nextUtc;
                    // Is interval job, not cron expression
                    if (int.TryParse((string) job.Parameters, out var millis))
                    {
                        var lastRun = value ?? DateTime.Now;
                        nextUtc = lastRun.AddMilliseconds(millis);
                    }
                    else
                    {
                        var expression = CronExpression.Parse((string) job.Parameters);
                        var occurence = value ?? DateTime.UtcNow;
                        nextUtc = expression.GetNextOccurrence(occurence);    
                    }
                    
                    if (!nextUtc.HasValue || nextUtc.Value > DateTime.UtcNow)
                    {
                        continue;
                    }

                    job.Type = JobType.Scheduled;
                    toExecute.Add(job);
                }

                if (toExecute.Any())
                {
                    await EnqueueMany(toExecute);
                }
            }
        }

        private async Task ExecuteJobRetry()
        {
            using (var retryLock = await _lockFactory.CreateLockAsync(
                $"{_options.KeyPrefix}_retry_lock", TimeSpan.FromMinutes(10)))
            {
                if (!retryLock.IsAcquired)
                {
                    return;
                }

                var key = $"{_options.KeyPrefix}_job_retries";

                var runIds = await _connection.GetDatabase().SetMembersAsync(key);

                var raw = await _connection.GetDatabase()
                    .StringGetAsync(runIds.Select(w => (RedisKey) $"{_options.KeyPrefix}_{w}_run").ToArray());

                var runs = raw.Where(w => w.HasValue).Select(w => BsonSerializer.FromBson<ExecutedJob>(w));

                var valid = runs.Where(w =>
                    w != null && w.Status == JobStatus.Retrying && DateTime.UtcNow.Ticks >= w.NextRetry);

                if (valid.Any())
                {
                    await EnqueueMany(valid);
                }
            }
        }

        private async Task OnImmediateJobFinish(Job job, ITransaction transaction)
        {
        }

        private async Task OnScheduledJobFinish(Job job, ITransaction transaction)
        {
            var lastCheck = $"{_options.KeyPrefix}_{job.Name}_last_check";
            await transaction.KeyDeleteAsync(lastCheck, CommandFlags.FireAndForget);
        }

        private async Task SaveJobRun(Job job, JobStatus status, ITransaction transaction)
        {
            var run = await GetRun(job.RunId.ToString());
            var execution = new ExecutedJob(job, status, _id);
            if (run != null)
            {
                execution.Retries = run.Retries;
                execution.NextRetry = run.NextRetry;
            }

            await SaveJobRun(execution, transaction);
        }

        private async Task SaveJobRun(ExecutedJob job, ITransaction transaction)
        {
            var last = await GetRun(job.RunId.ToString());
            if (last != null)
            {
                transaction.SetRemoveAsync($"{_options.KeyPrefix}_job_status_{last.Status.ToString()}",
                    last.RunId.ToString());
            }
            job.Timestamp = DateTime.UtcNow.Ticks;
            var lastRun = $"{_options.KeyPrefix}_{job.Name}_last_run";
            var key = $"{_options.KeyPrefix}_{job.Name}_runs";
            transaction.StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_run", BsonSerializer.ToBson(job));
            transaction.SetAddAsync(key, job.RunId.ToString());
            transaction.StringSetAsync(lastRun, job.Timestamp);
            transaction.SetAddAsync($"{_options.KeyPrefix}_job_status_{job.Status.ToString()}", job.RunId.ToString());
        }

        private async Task<ExecutedJob> GetRun(string runId)
        {
            var key = $"{_options.KeyPrefix}_{runId}_run";
            var value = await _connection.GetDatabase().StringGetAsync(key);
            return value.HasValue ? BsonSerializer.FromBson<ExecutedJob>(value) : null;
        }

        private async Task UpsertJob(string name, JobOptions options, ITransaction transaction = null)
        {
            var job = new Job
            {
                Name = name,
                Options = options
            };
            await UpsertJob(job, transaction);
        }

        private async Task UpsertJob(Job job, ITransaction transaction = null)
        {
            using (var redLock =
                await _lockFactory.CreateLockAsync($"{_options.KeyPrefix}_{job.Name}_create", TimeSpan.FromSeconds(5)))
            {
                if (!redLock.IsAcquired)
                {
                    return;
                }

                var db = _connection.GetDatabase();
                transaction = transaction ?? db.CreateTransaction();

                transaction.StringSetAsync($"{_options.KeyPrefix}_job_{job.Name}_meta", BsonSerializer.ToBson(job));
                transaction.SetAddAsync($"{_options.KeyPrefix}_jobs", job.Name);

                await transaction.ExecuteAsync();
            }
        }


        private async Task<Job> GetJob(string name)
        {
            var jobs = await GetJobs(new[] {name});
            return jobs.FirstOrDefault();
        }

        private async Task<IEnumerable<Job>> GetJobs(IEnumerable<string> names)
        {
            var keys = names.Select(w => (RedisKey) $"{_options.KeyPrefix}_job_{w}_meta").ToArray();
            var data = await _connection.GetDatabase().StringGetAsync(keys);
            return data.Where(w => w.HasValue).Select(w => BsonSerializer.FromBson<Job>(w));
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

        private async Task<IEnumerable<Job>> GetJobsOrThrow(List<string> names)
        {
            var jobs = await GetJobs(names);

            if (jobs.Count() != names.Count)
            {
                throw new Exception($"Failed to retrievd all jobs by names {string.Join(',', names)}. " +
                                    "Please add all jobs using .OnJob() method.");
            }

            return jobs;
        }
        
        private async Task OnJobStart(Job job, ITransaction transaction)
        {
            await SaveJobRun(job, JobStatus.Running, transaction);
            transaction.StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_details",
                BsonSerializer.ToBson(
                    new JobExecutionDetail
                    {
                        DateStarted = DateTime.UtcNow.Ticks,
                        Name = job.Name,
                        Parameters = job.Parameters,
                        Status = JobStatus.Running
                    }));
        }

        private async Task OnJobError(Job job, Exception e, ITransaction transaction)
        {
            transaction.ListRemoveAsync($"{_options.KeyPrefix}_progress", job.RunId.ToString());
            
            var run = await GetRun(job.RunId.ToString()) ?? new ExecutedJob(job, JobStatus.Retrying, _id);
            run.Exception = e;
            try
            {
                if (run.Retries < _options.MaxRetries && run.Status != JobStatus.Errored)
                {
                    await RetryJob(job, run, transaction);
                }
                else
                {
                    run.Status = JobStatus.Errored;
                    transaction.SetRemoveAsync($"{_options.KeyPrefix}_job_retries", job.RunId.ToString());
                    transaction.StringIncrementAsync($"{_options.KeyPrefix}_stats_total_job_errors");
                    await SaveJobRun(run, transaction);
                }
            }
            finally
            {
                await transaction.ExecuteAsync(CommandFlags.FireAndForget);
            }
        }

        private async Task<Dictionary<string, DateTime?>> GetLastRuns(IEnumerable<Job> job)
        {
            var jobsArray = job.ToArray();
            var keys = jobsArray.Select(w => (RedisKey) $"{_options.KeyPrefix}_{w.Name}_last_run").ToArray();
            var ticks = await _connection.GetDatabase().StringGetAsync(keys);
            var dict = new Dictionary<string, DateTime?>();
            for (var i = 0; i < jobsArray.Length; i++)
            {
                var tick = ticks[i];
                if (tick.HasValue)
                {
                    dict.Add(jobsArray[i].Name,
                        new DateTime(long.Parse(tick.ToString()), DateTimeKind.Utc));
                }
            }

            return dict;
        }

        private async Task<Dictionary<string, DateTime?>> GetLastChecks(IEnumerable<string> names)
        {
            var db = _connection.GetDatabase();
            var namesArray = names.ToArray();
            var keys = namesArray.Select(w => (RedisKey) $"{_options.KeyPrefix}_{w}_last_check").ToArray();
            var ticks = await db.StringGetAsync(keys);
            var now = DateTime.UtcNow;
            var dict = new Dictionary<string, DateTime?>();
            var hasNoValue = new List<string>();

            for (var i = 0; i < namesArray.Length; i++)
            {
                var tick = ticks[i];
                var name = namesArray[i];
                if (!tick.HasValue)
                {
                    hasNoValue.Add(name);
                }

                dict.Add(name,
                    tick.HasValue
                        ? new DateTime(long.Parse(tick.ToString()), DateTimeKind.Utc)
                        : new DateTime(now.Ticks, DateTimeKind.Utc));
            }

            var toSaveLastCheck = hasNoValue.Select(w => $"{_options.KeyPrefix}_{w}_last_check")
                .Select(w => new KeyValuePair<RedisKey, RedisValue>(w, now.Ticks)).ToArray();
            await db.StringSetAsync(toSaveLastCheck, When.Always, CommandFlags.FireAndForget);

            return dict;
        }

        private async Task RetryJob(Job job, ExecutedJob run, ITransaction transaction)
        {
            run.Retries++;
            run.Status = JobStatus.Retrying;
            run.NextRetry = CalculateNextRetry(run);
            await SaveJobRun(run, transaction);
            transaction.SetAddAsync($"{_options.KeyPrefix}_job_retries", job.RunId.ToString());
        }

        private async Task TrimJobRun()
        {
            using (var locking = await _lockFactory.CreateLockAsync($"{_options.KeyPrefix}_job_trimmer",
                TimeSpan.FromSeconds(30)))
            {
                if (!locking.IsAcquired)
                {
                    return;
                }

                var db = _connection.GetDatabase();
                var jobs = await db.SetMembersAsync($"{_options.KeyPrefix}_jobs");

                foreach (var job in jobs)
                {
                    var key = $"{_options.KeyPrefix}_{job}_runs";
                    var length = await db.SetLengthAsync(key);

                    if (length <= _options.MaxJobRunsToSave)
                    {
                        return;
                    }

                    var transaction = _connection.GetDatabase().CreateTransaction();
                    var members = await db.SetMembersAsync(key);
                    var successes = await db.SetMembersAsync($"{_options.KeyPrefix}_job_status_{JobStatus.Success.ToString()}");
                    var successSet = successes.Select(w => w.ToString()).ToHashSet();

                    var toRemove = members.Take((int) (length - _options.MaxJobRunsToSave));
                    foreach (var value in toRemove)
                    {
                        if (successSet.Contains(value.ToString()))
                        {
                            transaction.SetRemoveAsync(key, value);
                            transaction.KeyDeleteAsync($"{_options.KeyPrefix}_{value}_run");
                            transaction.KeyDeleteAsync($"{_options.KeyPrefix}_{value}_details");
                            transaction.SetRemoveAsync($"{_options.KeyPrefix}_job_status_{JobStatus.Success.ToString()}", value);
                        }
                    }

                    await transaction.ExecuteAsync(CommandFlags.FireAndForget);
                }
            }
        }


        private long CalculateNextRetry(ExecutedJob run)
        {
            return (run.Retries * _options.RetryBackOff).Ticks;
        }

        private async Task ExecuteQueue()
        {
            await _semaphore.WaitAsync();

            ExecutedJob job = null;
            string runId = string.Empty; 
            try
            {
                var db = _connection.GetDatabase();
                var trans = _connection.GetDatabase().CreateTransaction();

                var value = await db.ListRightPopLeftPushAsync($"{_options.KeyPrefix}_enqueued",
                    $"{_options.KeyPrefix}_progress");

                if (!value.HasValue)
                {
                    return;
                }

                runId = value.ToString();
                job = await GetRun(runId);

                if (job == null)
                {
                    throw new Exception($"Failed to find job run details relating to job run {runId}");
                }
                
                var key = $"{_options.KeyPrefix}_{job.Name}_lock";
                
                if (job.Options != null && !job.Options.AllowConcurrentExecution)
                {
                    using (var jobLock = await _lockFactory.CreateLockAsync(key, TimeSpan.FromSeconds(30)))
                    {
                        if (!jobLock.IsAcquired)
                        {
                            trans.ListRightPushAsync($"{_options.KeyPrefix}_enqueued", value);
                            trans.ListRemoveAsync($"{_options.KeyPrefix}_progress", value);
                            await trans.ExecuteAsync(CommandFlags.FireAndForget);
                            return;
                        }

                        await ExecuteJob(job, trans);
                    }
                }
                else
                {
                    await ExecuteJob(job, trans);
                }
            }
            catch (Exception e)
            {
                var trans = _connection.GetDatabase().CreateTransaction();
                if (_options.OnQueueError != null)
                {
                    await _options.OnQueueError(e);
                }

                if (job != null)
                {
                    await OnJobError(job, e, trans);
                    await trans.ExecuteAsync(CommandFlags.FireAndForget);
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

        private async Task ExecuteJob(ExecutedJob job, ITransaction transaction)
        {
            if (!_listeners.ContainsKey(job.Name) && !_listenersNoArgs.ContainsKey(job.Name))
            {
                await OnJobError(job, new NoHandlerFoundException(job), transaction);
                return;
            }

            await OnJobStart(job, transaction);
            await transaction.ExecuteAsync();

            transaction = _connection.GetDatabase().CreateTransaction();

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
                await OnJobError(job, e, transaction);
                return;
            }

            try
            {
                transaction.StringIncrementAsync($"{_options.KeyPrefix}_job_{job.Name}_run_count");
                transaction.ListRemoveAsync($"{_options.KeyPrefix}_progress", job.RunId.ToString());
                transaction.StringIncrementAsync($"{_options.KeyPrefix}_stats_total_runs");
                await SaveJobRun(job, JobStatus.Success, transaction);

                switch (job.Type)
                {
                    case JobType.Immediate:
                        await OnImmediateJobFinish(job, transaction);
                        break;
                    case JobType.Scheduled:
                        await OnScheduledJobFinish(job, transaction);
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                await transaction.ExecuteAsync(CommandFlags.FireAndForget);
            }
        }
    }
}