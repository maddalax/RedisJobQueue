using System;

namespace RedisJobQueue.Models.Exceptions
{
    [SerializableAttribute]
    public class NoHandlerFoundException : Exception
    {
        public NoHandlerFoundException(Job job) 
            : base($"No method handler was found for {job.Name}. Make sure you have called queue.OnJob('{job.Name}')")
        {
        }
    }
}