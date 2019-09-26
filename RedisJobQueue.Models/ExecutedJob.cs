using System;
using System.Globalization;

namespace RedisJobQueue.Models
{
    public class ExecutedJob
    {
        public string Name { get; set; }
        public long Timestamp { get; set; }
        public string MachineName { get; set; }
        public Guid MachineId { get; set; }
    }

    public class ExecutedJobDto
    {
        public string Name { get; }
        public string Timestamp { get; }
        public string MachineName { get; }
        public Guid MachineId { get; }

        public ExecutedJobDto(ExecutedJob job)
        {
            Name = job.Name;
            Timestamp = new DateTime(job.Timestamp, DateTimeKind.Utc).ToString(CultureInfo.InvariantCulture);
            MachineId = job.MachineId;
            MachineName = job.MachineName;
        }
    }
}