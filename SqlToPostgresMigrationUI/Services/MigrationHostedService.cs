using SqlToPostgresMigrationUI.Core.Models;
using SqlToPostgresMigrationUI.Core.Orchestrator;
using SqlToPostgresMigrationUI.Services.DTOs;
using SqlToPostgresMigrationUI.Services.SignalR;
using System.Collections.Concurrent;

namespace SqlToPostgresMigrationUI.Services;

public class MigrationHostedService : BackgroundService, IMigrationService
{
    private readonly ILogger<MigrationHostedService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IConfiguration _configuration;
    private readonly ConcurrentDictionary<string, MigrationJob> _activeMigrations = new();

    public event EventHandler<TableProgressDto>? TableProgressUpdated;
    public event EventHandler<MigrationCompletedEventArgs>? MigrationCompleted;

    public MigrationHostedService(
        ILogger<MigrationHostedService> logger,
        ILoggerFactory loggerFactory,
        IConfiguration configuration)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _configuration = configuration;
    }

    public async Task<string> StartMigrationAsync(
        MigrationRequestDto request,
        CancellationToken cancellationToken = default)
    {
        var migrationId = Guid.NewGuid().ToString();

        var options = new MigrationOptions
        {
            MaxParallelTables = request.MaxParallelTables,
            BatchSize = request.BatchSize,
            DryRun = request.DryRun,
            MaxRetries = request.MaxRetries,
            RetryDelaySeconds = request.RetryDelaySeconds,
            ValidateData = request.ValidateData
        };

        var job = new MigrationJob
        {
            Id = migrationId,
            Request = request,
            Options = options,
            Status = "Starting",
            StartTime = DateTime.UtcNow,
            CancellationTokenSource = new CancellationTokenSource()
        };

        _activeMigrations[migrationId] = job;

        // Run migration in background
        _ = Task.Run(async () => await ExecuteMigrationAsync(job), job.CancellationTokenSource.Token);

        return migrationId;
    }

    private async Task ExecuteMigrationAsync(MigrationJob job)
    {
        try
        {
            job.Status = "Running";

            var orchestrator = new MigrationOrchestrator(
                job.Request.SourceConnection,
                job.Request.TargetConnection,
                _loggerFactory,
                job.Options);

            // Subscribe to events
            orchestrator.ProgressUpdated += (s, p) =>
            {
                var dto = new TableProgressDto
                {
                    TableName = p.TableName,
                    RowsProcessed = p.RowsProcessed,
                    TotalRows = p.TotalRows,
                    Percentage = p.Percentage,
                    Status = p.Status,
                    MigrationId = job.Id
                };

                job.TableProgress[p.TableName] = dto;
                TableProgressUpdated?.Invoke(this, dto);
            };

            orchestrator.TableCompleted += (s, e) =>
            {
                _logger.LogInformation("Table {Table} completed", e.TableName);
            };

            // Execute migration
            var report = await orchestrator.MigrateAsync(job.CancellationTokenSource.Token);

            job.Report = report;
            job.Status = report.Status;
            job.EndTime = DateTime.UtcNow;

            MigrationCompleted?.Invoke(this, new MigrationCompletedEventArgs
            {
                MigrationId = job.Id,
                Success = report.Status == "Completed",
                Report = report
            });

            // Save state
            var statePath = Path.Combine(
                _configuration["Migration:StateFilePath"] ?? "migration-state",
                $"{job.Id}.json");

            await orchestrator.SaveStateAsync(report, statePath);
        }
        catch (OperationCanceledException)
        {
            job.Status = "Cancelled";
            job.EndTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Migration {MigrationId} failed", job.Id);
            job.Status = "Failed";
            job.ErrorMessage = ex.Message;
            job.EndTime = DateTime.UtcNow;
        }
    }

    public Task<bool> CancelMigrationAsync(string migrationId)
    {
        if (_activeMigrations.TryGetValue(migrationId, out var job))
        {
            job.CancellationTokenSource?.Cancel();
            job.Status = "Cancelling";
            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }

    public Task<MigrationStatusDto> GetMigrationStatusAsync(string migrationId)
    {
        if (_activeMigrations.TryGetValue(migrationId, out var job))
        {
            return Task.FromResult(new MigrationStatusDto
            {
                MigrationId = job.Id,
                Status = job.Status,
                StartTime = job.StartTime,
                EndTime = job.EndTime,
                Progress = job.TableProgress.Values.ToList(),
                ErrorMessage = job.ErrorMessage
            });
        }

        return Task.FromResult(new MigrationStatusDto
        {
            MigrationId = migrationId,
            Status = "NotFound"
        });
    }

    public Task<IEnumerable<TableProgressDto>> GetTableProgressAsync(string migrationId)
    {
        if (_activeMigrations.TryGetValue(migrationId, out var job))
        {
            return Task.FromResult(job.TableProgress.Values.AsEnumerable());
        }

        return Task.FromResult(Enumerable.Empty<TableProgressDto>());
    }

    public Task<IEnumerable<string>> GetAllActiveMigrationsAsync()
    {
        return Task.FromResult(_activeMigrations.Keys.AsEnumerable());
    }

    public Task<MigrationReport> GetMigrationReportAsync(string migrationId)
    {
        if (_activeMigrations.TryGetValue(migrationId, out var job) && job.Report != null)
        {
            return Task.FromResult(job.Report);
        }

        return Task.FromResult(new MigrationReport());
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Background task to clean up old migrations
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);

                // Remove completed migrations older than 1 hour
                var oldMigrations = _activeMigrations.Values
                    .Where(j => j.EndTime.HasValue &&
                               (DateTime.UtcNow - j.EndTime.Value) > TimeSpan.FromHours(1))
                    .Select(j => j.Id)
                    .ToList();

                foreach (var id in oldMigrations)
                {
                    _activeMigrations.TryRemove(id, out _);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown or service stop, swallow and exit gracefully.
        }
    }
}
