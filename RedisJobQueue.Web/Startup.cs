using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SpaServices.ReactDevelopmentServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RedisJobQueue.Models;
using StackExchange.Redis;

namespace RedisJobQueue.Web
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

            // In production, the React files will be served from this directory
            services.AddSpaStaticFiles(configuration => { configuration.RootPath = "ClientApp/build"; });

            services.AddSingleton(provider =>
            {
                var conn = ConnectionMultiplexer
                    .Connect(
                        Configuration.GetConnectionString("Redis"));
                return new RedisJobQueue(conn, new JobQueueOptions
                {
                    Namespace = Configuration.GetValue<string>("RedisJobQueue:Namespace")
                });
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();
            app.UseSpaStaticFiles();

            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller}/{action=Index}/{id?}");
            });

            app.UseSpa(spa =>
            {
                spa.Options.SourcePath = "ClientApp";

                if (env.IsDevelopment())
                {
                    spa.UseReactDevelopmentServer(npmScript: "start");
                }
            });
            
            RegisterQueue(app.ApplicationServices.GetService<RedisJobQueue>());
        }

        private void RegisterQueue(RedisJobQueue queue)
        {
           queue.Queue.OnJob("test", async () =>
           {
               Console.WriteLine(Guid.NewGuid());
               await Task.Delay(2000);
           });
           
           queue.Queue.OnJob("error_test", async () =>
           {
              throw new Exception("error has occured.");
           });

           queue.Queue.OnJob("scheduled", async () =>
           {
               Console.WriteLine("scheduled has been executed.");
           });
           
           queue.Queue.OnJob("scheduled_error", async () =>
           {
               throw new Exception("scheduled error has occured.");
           });
           
           queue.Queue.OnJob("concurrent (only allows 1 at time)", new JobOptions
           {
               AllowConcurrentExecution = false
           }, async () =>
           {
               Console.WriteLine("Concurrent: " + DateTime.UtcNow);
               Console.WriteLine("Sleeping 2s.");
               await Task.Delay(2000);
           });
           
           queue.Queue.OnJob("not concurrent (allows parallel execution) (10 workers default)", new JobOptions
           {
               AllowConcurrentExecution = true
           }, async () =>
           {
               Console.WriteLine("Not Concurrent: " + DateTime.UtcNow);
               Console.WriteLine("Sleeping 2s.");
               await Task.Delay(2000);
           });
           
           queue.Queue.Start();
        }
    }
}