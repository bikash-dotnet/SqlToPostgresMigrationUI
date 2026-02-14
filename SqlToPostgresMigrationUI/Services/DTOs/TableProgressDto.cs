namespace SqlToPostgresMigrationUI.Services.DTOs;

public class TableProgressDto
{
    public string MigrationId { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public long RowsProcessed { get; set; }
    public long TotalRows { get; set; }
    public double Percentage { get; set; }
    public string Status { get; set; } = string.Empty;
    public TimeSpan ElapsedTime { get; set; }
    public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
}
