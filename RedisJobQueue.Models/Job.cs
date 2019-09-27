using System;

namespace RedisJobQueue.Models
{
    public class Job
    {
        public string Name { get; set; }
        public object Parameters { get; set; }
        public JobType Type { get; set; }
        public Guid RunId { get; set; }
        
        public JobOptions Options { get; set; }

        public Job()
        {
            
        }

        public Job(ExecutedJob run)
        {
            Name = run.Name;
            Parameters = run.Parameters;
            Type = run.Type;
            RunId = run.RunId;
        }
    }

    public enum JobType
    {
        Immediate,
        Scheduled
    }
}