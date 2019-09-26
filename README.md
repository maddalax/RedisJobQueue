Job queue backed by Redis for scaling background jobs over multiple nodes. 

**Current Status:** Work in progress, heavy development still needed.

**Current features:**
- Retry upon error thrown. Default is 10 times with a 5 second exponential back-off. (Back off rate is currently bugged. Just does 5 seconds between executions now.)
- Scheduled Jobs (default 5 second poll rate)
- Immediate jobs
- Dashboard to view and manage jobs.
- REST API to view and manage jobs.

**Usage:**

```c#
            var queue = new RedisJobQueue(ConnectionMultiplexer.Connect("localhost"), new JobQueueOptions
            {
                Namespace = "your_app_name"
            }); 
            
           // No parameters. 
           queue.Queue.OnJob("my_job",  () =>
           {
               Console.WriteLine("my job has been executed.");
               return Task.CompletedTask;
           });

           // With parameters.
           queue.Queue.OnJob<string>("my_job_with_parameters", value =>
           {
              Console.WriteLine("my job with parameters: " + value);
              return Task.CompletedTask;
           });
           
           queue.Queue.OnJob("my_scheduled_job", () =>
           {
               Console.WriteLine("My scheduled job has been executed.");
               return Task.CompletedTask;
           });
           
           // Start the queue to setup the listeners, must be done before enqueue is called.
           queue.Queue.Start();
           
           queue.Queue.Enqueue("my_job");
           
           queue.Queue.Enqueue("my_job_with_parameters", "my parameter");

           // Schedule a job to run every minute. https://crontab.guru/#*_*_*_*_*
           queue.Queue.Schedule("my_scheduled_job", "* * * * *");
```

**Dashboard:** 

![](https://d.pr/i/TquJ9l+)

