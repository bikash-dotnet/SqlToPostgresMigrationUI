using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.Options;
using SqlToPostgresMigrationUI.Core.Models;
using SqlToPostgresMigrationUI.Core.Orchestrator;
using SqlToPostgresMigrationUI.Services;
using SqlToPostgresMigrationUI.Services.SignalR;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddSignalR(); // For real-time updates
builder.Services.AddLogging(config =>
{
    config.AddConsole();
    config.AddDebug();
});


// bind options first (optional)
builder.Services.Configure<MigrationOptions>(builder.Configuration.GetSection("MigrationOptions"));

// Register Services
builder.Services.AddSingleton<IMigrationService, MigrationHostedService>();
builder.Services.AddHostedService<MigrationHostedService>(sp =>
    (MigrationHostedService)sp.GetRequiredService<IMigrationService>());
builder.Services.AddSingleton<MigrationStateService>();
builder.Services.AddScoped<MigrationOrchestrator>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
    var options = sp.GetService<IOptions<MigrationOptions>>()?.Value;
    var source = config.GetConnectionString("SqlServer");
    var target = config.GetConnectionString("Postgres");

    return new MigrationOrchestrator(source, target, loggerFactory,options);
});
// Configure connections
builder.Configuration.AddJsonFile("appsettings.json", optional: false);
builder.Configuration.AddUserSecrets<Program>(); // For sensitive data

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.MapBlazorHub();
app.MapHub<MigrationHub>("/migrationHub"); // Map SignalR hub
app.MapFallbackToPage("/_Host");
// Create state directory if not exists
var statePath = Path.Combine(Directory.GetCurrentDirectory(), "migration-state");
if (!Directory.Exists(statePath))
{
    Directory.CreateDirectory(statePath);
}
app.Run();
