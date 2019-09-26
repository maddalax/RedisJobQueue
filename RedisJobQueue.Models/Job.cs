namespace RedisJobQueue.Models
{
    public class Job
    {
        public string Name { get; set; }
        public object Parameters { get; set; }
        public JobType Type { get; set; }
    }

    public enum JobType
    {
        Immediate,
        Scheduled
    }
}