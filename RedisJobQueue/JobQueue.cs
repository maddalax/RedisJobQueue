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
            _subscriber.SubscribeAsync(_options.KeyPrefix, async delegate
            {
                  try
                  {
                      var value = await _connection.GetDatabase().ListLeftPopAsync($"{_options.KeyPrefix}_enqueued");
                      if (!value.HasValue)
                      {
                          return;
                      }
                      var job = BsonSerializer.FromBson<Job>(value);
                      var key = $"{_options.KeyPrefix}_{job.Name}";
                    
                      using (var redLock = await _lockFactory.CreateLockAsync(key, _options.JobLockTimeout))
                      {
                          if (!redLock.IsAcquired)
                          {
                              return;
                          }

                          if (!_listeners.ContainsKey(job.Name) && !_listenersNoArgs.ContainsKey(job.Name))
                          {
                              await OnJobError(job, new NoHandlerFoundException(job));
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
                              return;
                          }
                        
                          await SaveJobRun(job, JobStatus.Success);

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
                  catch (Exception e)
                  {
                      if (_options.OnQueueError != null)
                      {
                          await _options.OnQueueError(e);
                      }
                      else
                      {
                          throw;
                      }
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
           _executor.Submit(ExecutePoll);
           _executor.Submit(ExecuteScheduledJobs);
           _executor.Submit(ExecuteJobRetry);
        }

        public async Task Enqueue(string job, object args)
        {
            await Enqueue(new Job
            {
                Name = job,
                Parameters = args
            });
        }
        
        public async Task Enqueue(string job)
        {
            await Enqueue(new Job { Name = job });
        }
        
        private async Task Enqueue(Job job)
        {
            job.RunId = Guid.NewGuid();
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_jobs", job.Name);
            var serialized = BsonSerializer.ToBson(job);
            await SaveJobRun(job, JobStatus.Enqueued);
            await _connection.GetDatabase().ListRightPushAsync($"{_options.KeyPrefix}_enqueued", serialized);
            await _subscriber.PublishAsync(_options.KeyPrefix, "");
        }
        
        private async Task Enqueue(ExecutedJob run)
        {
            await _connection.GetDatabase().SetAddAsync($"{_options.KeyPrefix}_jobs", run.Name);
            run.Status = JobStatus.Enqueued;
            await SaveJobRun(run);
            var serialized = BsonSerializer.ToBson(new Job(run));
            await _connection.GetDatabase().ListRightPushAsync($"{_options.KeyPrefix}_enqueued", serialized, When.Always, CommandFlags.FireAndForget);
            await _subscriber.PublishAsync(_options.KeyPrefix, "");
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

        private async Task ExecutePoll()
        {
            await _subscriber.PublishAsync(_options.KeyPrefix, "");
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
            await _connection.GetDatabase().StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_run", BsonSerializer.ToBson(job));
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

        private async Task OnJobStart(Job job)
        {
            await SaveJobRun(job, JobStatus.Running);
            await _connection.GetDatabase().StringSetAsync($"{_options.KeyPrefix}_{job.RunId}_details", BsonSerializer.ToBson(
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

    }
}