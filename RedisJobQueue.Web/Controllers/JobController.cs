using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using RedisJobQueue.Models;

namespace RedisJobQueue.Web.Controllers
{
    [Route("api/[controller]/[action]")]
    public class JobController : Controller
    {
        private readonly RedisJobQueue _store;

        public JobController(RedisJobQueue store)
        {
            _store = store;
        }

        public async Task<HashSet<string>> Jobs()
        {
            return await _store.Analytics.GetJobs();
        }
        
        public async Task<IActionResult> Enqueue([FromQuery] string name, int count = 1)
        {
            var list = new List<string>(count);
            for (var i = 0; i < count; i++)
            {
                list.Add(name);
            }

            await _store.Queue.EnqueueMany(list);
            return NoContent();
        }
        
        public async Task<IActionResult> EnqueuedCount()
        {
            var count = await _store.Analytics.GetEnqueuedCount();
            return Ok(count);
        }
        
        public async Task<IEnumerable<ExecutedJobDto>> Runs([FromQuery] string job)
        {
            var runs = await _store.Analytics.GetRuns(job);
            return runs
                .OrderByDescending(w => w.Timestamp)
                .Select(w => new ExecutedJobDto(w));
        }
    }
}