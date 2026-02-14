namespace SqlToPostgresMigrationUI.Core.Models;

public class MigrationOptions
{
    public int MaxParallelTables { get; set; } = 4;
    public int BatchSize { get; set; } = 10000;
    public int MaxRetries { get; set; } = 3;
    public int RetryDelaySeconds { get; set; } = 5;
    public bool DryRun { get; set; } = false;
    public bool ValidateData { get; set; } = true;
}
