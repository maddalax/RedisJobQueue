using System;
using System.Threading.Tasks;

namespace RedisJobQueue.Models
{
    public class JobQueueOptions
    {
        private string _keyPrefix;
        public string Namespace { get; set; }
        public TimeSpan JobLockTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan PollRate { get; set; } = TimeSpan.FromSeconds(5);

        public int MaxRetries { get; set; } = 10;
        
        public TimeSpan RetryBackOff { get; set; } = TimeSpan.FromSeconds(5);

        public Func<Exception, Task> OnQueueError { get; set; }

        public string KeyPrefix
        {
            get
            {
                if (_keyPrefix != null)
                {
                    return _keyPrefix;
                }
                var key = "redis_job_queue";
                if (!string.IsNullOrEmpty(Namespace))
                {
                    key = $"{Namespace}_{key}";
                }
                _keyPrefix = key;
                return key;
            }
        }
    }
}