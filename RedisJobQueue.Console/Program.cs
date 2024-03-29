﻿
using System;
using System.Threading.Tasks;
using RedisJobQueue.Models;
using StackExchange.Redis;

namespace RedisJobQueue.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var conn = ConnectionMultiplexer
                .Connect("localhost:6379,abortConnect=false");
            var queue = new RedisJobQueue(conn, new JobQueueOptions
            {
                Namespace = "maddev",
                PollRate = TimeSpan.FromSeconds(1)
            });
            queue.Queue.OnScheduledJob("test_interval", () =>
            {
                System.Console.WriteLine(Guid.NewGuid() + " " + DateTime.UtcNow); 
                return Task.CompletedTask;
            });
            queue.Queue.Start();
            queue.Queue.Interval("test_interval", TimeSpan.FromSeconds(1));
            System.Console.ReadLine();
        }
    }
}