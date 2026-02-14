namespace SqlToPostgresMigrationUI.Core.Models;

public class DatabaseSchema
{
    public List<TableSchema> Tables { get; set; } = new();
    public List<ForeignKeySchema> ForeignKeys { get; set; } = new();
    public Dictionary<string, string> TypeMappings { get; set; } = new();
}

public class TableSchema
{
    public string SourceSchema { get; set; } = "dbo";
    public string SourceName { get; set; } = string.Empty;
    public string TargetSchema { get; set; } = "public";
    public string TargetName { get; set; } = string.Empty;
    public List<ColumnSchema> Columns { get; set; } = new();
    public List<IndexSchema> Indexes { get; set; } = new();
    public List<ForeignKeySchema> ForeignKeys { get; set; } = new();
    public PrimaryKeySchema? PrimaryKey { get; set; }
    public long RowCount { get; set; }
    public MigrationStatus Status { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime? MigratedAt { get; set; }
}

public class ColumnSchema
{
    public string Name { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public string TargetType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public object? DefaultValue { get; set; }
    public bool IsIdentity { get; set; }
    public string? IdentitySeed { get; set; }
    public string? IdentityIncrement { get; set; }
    public int OrdinalPosition { get; set; }
}

public class PrimaryKeySchema
{
    public string Name { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
}

public class IndexSchema
{
    public string Name { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public bool IsUnique { get; set; }
    public bool IsClustered { get; set; }
    public string? Filter { get; set; }
}

public class ForeignKeySchema
{
    public string Name { get; set; } = string.Empty;
    public string SourceTable { get; set; } = string.Empty;
    public string SourceSchema { get; set; } = string.Empty;
    public List<string> SourceColumns { get; set; } = new();
    public string TargetTable { get; set; } = string.Empty;
    public string TargetSchema { get; set; } = string.Empty;
    public List<string> TargetColumns { get; set; } = new();
    public string UpdateRule { get; set; } = "NO ACTION";
    public string DeleteRule { get; set; } = "NO ACTION";
}

public enum MigrationStatus
{
    Pending,
    Migrating,
    Completed,
    Failed,
    Validated
}

public class MigrationProgress
{
    public string TableName { get; set; } = string.Empty;
    public long RowsProcessed { get; set; }
    public long TotalRows { get; set; }
    public double Percentage => TotalRows > 0 ? (RowsProcessed * 100.0 / TotalRows) : 0;
    public string Status { get; set; } = string.Empty;
    public TimeSpan ElapsedTime { get; set; }
}
