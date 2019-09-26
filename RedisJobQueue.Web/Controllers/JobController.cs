using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using RedisJobQueue.Models;

namespace RedisJobQueue.Web.Controllers
{
    [Route("api/[controller]")]
    public class JobController : Controller
    {
        private readonly RedisJobQueue _store;

        public JobController(RedisJobQueue store)
        {
            _store = store;
        }

        [HttpGet("[action]")]
        public async Task<HashSet<string>> Jobs()
        {
            return await _store.Analytics.GetJobs();
        }
        
        [HttpGet("[action]")]
        public async Task<IEnumerable<ExecutedJobDto>> Runs([FromQuery] string job)
        {
            var runs = await _store.Analytics.GetRuns(job);
            return runs
                .OrderByDescending(w => w.Timestamp)
                .Select(w => new ExecutedJobDto(w));
        }
    }
}