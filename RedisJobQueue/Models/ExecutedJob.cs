using System;
using System.Globalization;
using Newtonsoft.Json;

namespace RedisJobQueue.Models
{
    public class ExecutedJob : Job
    {
        public long Timestamp { get; set; }
        public string MachineName { get; set; }
        public Guid MachineId { get; set; }
        public JobStatus Status { get; set; }
        
        public long NextRetry { get; set; }

        public int Retries { get; set; }
        public Exception Exception { get; set; }
        public ExecutedJob()
        {
            
        }

        public ExecutedJob(Job job, JobStatus status, Guid machineId)
        {
            MachineId = machineId;
            MachineName = Environment.MachineName;
            Name = job.Name;
            RunId = job.RunId;
            Parameters = job.Parameters;
            Status = status;
            Type = job.Type;
            Options = job.Options;
        }
    }

    public class ExecutedJobDto
    {
        public string Name { get; }
        public string Timestamp { get; }
        public string MachineName { get; }
        public Guid MachineId { get; }
        public Guid RunId { get; }
        public string Status { get; }
        public int Retries { get; }
        public string Parameters { get; set; }
        public string Exception { get; set; }

        public ExecutedJobDto(ExecutedJob job)
        {
            Name = job.Name;
            Timestamp = new DateTime(job.Timestamp, DateTimeKind.Utc).ToString(CultureInfo.InvariantCulture);
            MachineId = job.MachineId;
            MachineName = job.MachineName;
            RunId = job.RunId;
            Status = job.Status.ToString();
            Retries = job.Retries;
            Parameters = JsonConvert.SerializeObject(Parameters);
            Exception = job.Exception?.ToString();
        }
    }
}