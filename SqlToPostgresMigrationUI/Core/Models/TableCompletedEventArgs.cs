namespace SqlToPostgresMigrationUI.Core.Models;

public class TableCompletedEventArgs : EventArgs
{
    public string TableName { get; set; } = string.Empty;
    public long RowsMigrated { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
}
