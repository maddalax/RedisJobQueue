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
            
            RegisterQueue(app.ApplicationServices.GetService<IServiceScopeFactory>());
        }

        private async Task RegisterQueue(IServiceScopeFactory factory)
        {
            using (var scope = factory.CreateScope())
            {
                try
                {
                    var queue = scope.ServiceProvider.GetService<RedisJobQueue>();
                    await queue.Queue.OnJob<int>("test", async value =>
                    {
                        Console.WriteLine(value);
                        await Task.Delay(2000);
                    });
                    queue.Queue.OnScheduledJob("scheduled",
                        async () => { Console.WriteLine("Scheduled executed."); });
                    queue.Queue.Start();
                    await queue.Queue.Schedule("scheduled", "* * * * *");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }
    }
}