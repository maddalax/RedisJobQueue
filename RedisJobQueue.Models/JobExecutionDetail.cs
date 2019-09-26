using System;

namespace RedisJobQueue.Models
{
    public class JobExecutionDetail
    {
        public string Name { get; set; }
        public JobStatus Status { get; set; }
        public long DateStarted { get; set; }
        public object Parameters { get; set; }
        
        public Exception Exception { get; set; }
    }

    public enum JobStatus
    {
        Running,
        Errored,
        Stopped,
        Expired
    }
}