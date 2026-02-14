namespace SqlToPostgresMigrationUI.Core.Models;

public class MigrationReport
{
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Status { get; set; } = "Pending";
    public string? ErrorMessage { get; set; }
    public int SourceTables { get; set; }
    public long SourceRowCount { get; set; }
    public int TablesCreated { get; set; }
    public int ForeignKeysCreated { get; set; }
    public int TablesValidated { get; set; }
    public int TablesFailedValidation { get; set; }
    public List<TableValidationResult> Validations { get; set; } = new();
    public Dictionary<string, object> Metadata { get; set; } = new();

    public TimeSpan Duration => (EndTime ?? DateTime.UtcNow) - StartTime;
}
