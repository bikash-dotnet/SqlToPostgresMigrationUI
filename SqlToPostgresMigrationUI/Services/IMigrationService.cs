using SqlToPostgresMigrationUI.Core.Models;
using SqlToPostgresMigrationUI.Services.DTOs;
using System.Collections.Concurrent;

namespace SqlToPostgresMigrationUI.Services;

public interface IMigrationService
{
    // Start a new migration
    Task<string> StartMigrationAsync(MigrationRequestDto request, CancellationToken cancellationToken = default);

    // Cancel an ongoing migration
    Task<bool> CancelMigrationAsync(string migrationId);

    // Get status of a migration
    Task<MigrationStatusDto> GetMigrationStatusAsync(string migrationId);

    // Get progress for all tables
    Task<IEnumerable<TableProgressDto>> GetTableProgressAsync(string migrationId);

    // Get all active migrations
    Task<IEnumerable<string>> GetAllActiveMigrationsAsync();

    // Get final report
    Task<MigrationReport> GetMigrationReportAsync(string migrationId);

    // Events for real-time updates
    event EventHandler<TableProgressDto> TableProgressUpdated;
    event EventHandler<MigrationCompletedEventArgs> MigrationCompleted;
}

// Supporting classes
public class MigrationJob
{
    public string Id { get; set; } = string.Empty;
    public MigrationRequestDto Request { get; set; } = null!;
    public MigrationOptions Options { get; set; } = null!;
    public string Status { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string? ErrorMessage { get; set; }
    public CancellationTokenSource? CancellationTokenSource { get; set; }
    public ConcurrentDictionary<string, TableProgressDto> TableProgress { get; set; } = new();
    public MigrationReport? Report { get; set; }
}

public class MigrationStatusDto
{
    public string MigrationId { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public List<TableProgressDto> Progress { get; set; } = new();
    public string? ErrorMessage { get; set; }
}

public class MigrationCompletedEventArgs : EventArgs
{
    public string MigrationId { get; set; } = string.Empty;
    public bool Success { get; set; }
    public MigrationReport? Report { get; set; }
}
