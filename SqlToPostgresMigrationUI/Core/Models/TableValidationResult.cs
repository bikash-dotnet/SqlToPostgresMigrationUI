namespace SqlToPostgresMigrationUI.Core.Models;

public class TableValidationResult
{
    public string TableName { get; set; } = string.Empty;
    public long SourceRowCount { get; set; }
    public long TargetRowCount { get; set; }
    public string SourceChecksum { get; set; } = string.Empty;
    public string TargetChecksum { get; set; } = string.Empty;
    public bool IsValid { get; set; }
}
