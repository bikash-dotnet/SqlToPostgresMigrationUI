namespace SqlToPostgresMigrationUI.Services.DTOs;

public class MigrationRequestDto
{
    public string SourceConnection { get; set; } = string.Empty;
    public string TargetConnection { get; set; } = string.Empty;
    public bool DryRun { get; set; }
    public int MaxParallelTables { get; set; } = 4;
    public int BatchSize { get; set; } = 10000;
    public int MaxRetries { get; set; } = 3;
    public int RetryDelaySeconds { get; set; } = 5;
    public bool ValidateData { get; set; } = true;
    public List<string>? IncludeTables { get; set; } // Optional: only migrate specific tables
    public List<string>? ExcludeTables { get; set; } // Optional: exclude specific tables
}
