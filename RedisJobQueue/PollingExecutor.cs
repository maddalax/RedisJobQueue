using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RedisJobQueue
{
    public class PollingExecutor
    {
        private readonly TimeSpan _rate;
        private CancellationToken? _token;
        private readonly List<Func<Task>> _tasks;
            
        public PollingExecutor(TimeSpan rate)
        {
            _rate = rate;
            _tasks = new List<Func<Task>>();
        }

        public void Submit(Func<Task> func)
        {
            _tasks.Add(func);
        }

        public void Start(CancellationToken? token)
        {
            _token = token;
            Task.Factory.StartNew(Execute, TaskCreationOptions.LongRunning);
        }

        private async Task Execute()
        {
            while (true)
            {
                try
                {
                    if (_token.HasValue && _token.Value.IsCancellationRequested)
                    {
                        break;
                    }

                    await DoExecute();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                finally
                {
                    await Task.Delay(_rate);
                }
            }
        }

        private async Task DoExecute()
        {
            await Task.WhenAll(_tasks.Select(w => w.Invoke()));
        }
    }
}