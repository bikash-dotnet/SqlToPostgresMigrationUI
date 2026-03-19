using Microsoft.Data.SqlClient;
using SqlToPostgresMigrationUI.Core.Models;
using System.Data;
using System.Runtime.CompilerServices;

namespace SqlToPostgresMigrationUI.Core.Readers;

public class SqlServerReader : IDisposable
{
    private readonly string _connectionString;
    private SqlConnection? _connection;
    private readonly ILogger<SqlServerReader> _logger;

    public SqlServerReader(string connectionString, ILogger<SqlServerReader> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task<DatabaseSchema> ReadSchemaAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectionAsync(cancellationToken);

        var schema = new DatabaseSchema
        {
            Tables = await ReadTablesAsync(cancellationToken),
            ForeignKeys = await ReadForeignKeysAsync(cancellationToken),
            TypeMappings = GetTypeMappings()
        };

        return schema;
    }

    private async Task<List<TableSchema>> ReadTablesAsync(CancellationToken cancellationToken)
    {
        var tables = new List<TableSchema>();

        // Get all user tables
        var sql = @"
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) as ColumnCount
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME";

        using var command = new SqlCommand(sql, _connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var table = new TableSchema
            {
                SourceSchema = reader.GetString(0),
                SourceName = reader.GetString(1),
                TargetSchema = MapSchema(reader.GetString(0)), // dbo → public
                TargetName = reader.GetString(1),
                Columns = await ReadColumnsAsync(reader.GetString(0), reader.GetString(1), cancellationToken),
                Indexes = await ReadIndexesAsync(reader.GetString(0), reader.GetString(1), cancellationToken),
                PrimaryKey = await ReadPrimaryKeyAsync(reader.GetString(0), reader.GetString(1), cancellationToken),
                RowCount = await GetRowCountAsync(reader.GetString(0), reader.GetString(1), cancellationToken)
            };

            tables.Add(table);
        }

        return tables;
    }

    private async Task<List<ColumnSchema>> ReadColumnsAsync(
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var columns = new List<ColumnSchema>();

        var sql = @"
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.COLUMN_DEFAULT,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IsIdentity,
                IDENT_SEED(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) as IdentitySeed,
                IDENT_INCR(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) as IdentityIncrement,
                c.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = @Schema AND c.TABLE_NAME = @Table
            ORDER BY c.ORDINAL_POSITION";

        using var command = new SqlCommand(sql, _connection);
        command.Parameters.AddWithValue("@Schema", schema);
        command.Parameters.AddWithValue("@Table", table);

        try
        {
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var column = new ColumnSchema
                {
                    Name = reader.GetString(0),
                    SourceType = reader.GetString(1),
                    TargetType = MapSqlTypeToPostgres(reader.GetString(1)),
                    IsNullable = reader.GetString(2) == "YES",
                    MaxLength = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    Precision = reader.IsDBNull(4) ? null : reader.GetByte(4),
                    Scale = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    DefaultValue = reader.IsDBNull(6) ? null : reader.GetString(6),
                    IsIdentity = reader.IsDBNull(7) ? false : reader.GetInt32(7) == 1,
                    IdentitySeed = reader.IsDBNull(8) ? null : reader.GetDecimal(8).ToString(),
                    IdentityIncrement = reader.IsDBNull(9) ? null : reader.GetDecimal(9).ToString(),
                    OrdinalPosition = reader.GetInt32(10)
                };

                columns.Add(column);
            }
        }
        catch(Exception ex)
        {
            _logger.LogDebug($"ReadColumnsAsync failed for {table}", ex);
            throw;
        }
        

        return columns;
    }

    private async Task<PrimaryKeySchema?> ReadPrimaryKeyAsync(
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var sql = @"
            SELECT 
                kc.CONSTRAINT_NAME,
                c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc 
                ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.COLUMNS c
                ON kc.TABLE_SCHEMA = c.TABLE_SCHEMA 
                AND kc.TABLE_NAME = c.TABLE_NAME 
                AND kc.COLUMN_NAME = c.COLUMN_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = @Schema
                AND tc.TABLE_NAME = @Table
            ORDER BY kc.ORDINAL_POSITION";

        using var command = new SqlCommand(sql, _connection);
        command.Parameters.AddWithValue("@Schema", schema);
        command.Parameters.AddWithValue("@Table", table);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var columns = new List<string>();
        string? constraintName = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            constraintName = reader.GetString(0);
            columns.Add(reader.GetString(1));
        }

        return columns.Any()
            ? new PrimaryKeySchema { Name = constraintName!, Columns = columns }
            : null;
    }

    private async Task<List<IndexSchema>> ReadIndexesAsync(
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var indexes = new List<IndexSchema>();

        var sql = @"
            SELECT 
                i.name as IndexName,
                i.is_unique,
                i.is_primary_key,
                c.name as ColumnName
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @Schema 
                AND t.name = @Table
                AND i.is_primary_key = 0  -- Skip PK as they're handled separately
            ORDER BY i.name, ic.key_ordinal";

        using var command = new SqlCommand(sql, _connection);
        command.Parameters.AddWithValue("@Schema", schema);
        command.Parameters.AddWithValue("@Table", table);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var indexGroups = new Dictionary<string, IndexSchema>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var indexName = reader.GetString(0);

            if (!indexGroups.ContainsKey(indexName))
            {
                indexGroups[indexName] = new IndexSchema
                {
                    Name = indexName,
                    IsUnique = reader.GetBoolean(1),
                    Columns = new List<string>()
                };
            }

            indexGroups[indexName].Columns.Add(reader.GetString(3));
        }

        return indexGroups.Values.ToList();
    }

    private async Task<List<ForeignKeySchema>> ReadForeignKeysAsync(CancellationToken cancellationToken)
    {
        var foreignKeys = new List<ForeignKeySchema>();

        var sql = @"
            SELECT 
                fk.name AS FK_Name,
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS Source_Schema,
                OBJECT_NAME(fk.parent_object_id) AS Source_Table,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS Target_Schema,
                OBJECT_NAME(fk.referenced_object_id) AS Target_Table,
                col_name(fkc.parent_object_id, fkc.parent_column_id) AS Source_Column,
                col_name(fkc.referenced_object_id, fkc.referenced_column_id) AS Target_Column,
                fk.update_referential_action_desc,
                fk.delete_referential_action_desc
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc 
                ON fk.object_id = fkc.constraint_object_id
            ORDER BY fk.name, fkc.constraint_column_id";

        using var command = new SqlCommand(sql, _connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var fkGroups = new Dictionary<string, ForeignKeySchema>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var fkName = reader.GetString(0);

            if (!fkGroups.ContainsKey(fkName))
            {
                fkGroups[fkName] = new ForeignKeySchema
                {
                    Name = fkName,
                    SourceSchema = reader.GetString(1),
                    SourceTable = reader.GetString(2),
                    TargetSchema = reader.GetString(3),
                    TargetTable = reader.GetString(4),
                    SourceColumns = new List<string>(),
                    TargetColumns = new List<string>(),
                    UpdateRule = MapReferentialAction(reader.GetString(7)),
                    DeleteRule = MapReferentialAction(reader.GetString(8))
                };
            }

            fkGroups[fkName].SourceColumns.Add(reader.GetString(5));
            fkGroups[fkName].TargetColumns.Add(reader.GetString(6));
        }

        return fkGroups.Values.ToList();
    }

    public async IAsyncEnumerable<object[]> StreamTableDataAsync(
        TableSchema table,
        int batchSize = 10000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Build explicit column list to ensure order matches schema
        var columnNames = string.Join(", ", table.Columns.OrderBy(c => c.OrdinalPosition).Select(c => $"[{c.Name}]"));
        var sql = $"SELECT {columnNames} FROM [{table.SourceSchema}].[{table.SourceName}]";

        using var command = new SqlCommand(sql, _connection);
        command.CommandTimeout = 300; // 5 minutes

        using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        // Validate field count matches schema
        if (reader.FieldCount != table.Columns.Count)
        {
            _logger.LogError(
                "Column count mismatch for {Table}: Reader has {ReaderCount} fields but schema has {SchemaCount} columns",
                table.SourceName, reader.FieldCount, table.Columns.Count);
            throw new InvalidOperationException(
                $"Column count mismatch: reader returned {reader.FieldCount} fields but schema has {table.Columns.Count} columns");
        }

        var batch = new List<object[]>(batchSize);

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            batch.Add(row);

            if (batch.Count >= batchSize)
            {
                foreach (var r in batch)
                    yield return r;
                batch.Clear();
            }
        }

        foreach (var r in batch)
            yield return r;
    }

    private async Task<long> GetRowCountAsync(string schema, string table, CancellationToken cancellationToken)
    {
        var sql = $"SELECT COUNT(*) FROM [{schema}].[{table}]";
        using var command = new SqlCommand(sql, _connection);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    private async Task EnsureConnectionAsync(CancellationToken cancellationToken)
    {
        _connection ??= new SqlConnection(_connectionString);

        if (_connection.State != ConnectionState.Open)
            await _connection.OpenAsync(cancellationToken);
    }

    private string MapSchema(string sourceSchema) => sourceSchema == "dbo" ? "public" : sourceSchema;

    private string MapSqlTypeToPostgres(string sqlType) => sqlType.ToLower() switch
    {
        "int" => "integer",
        "bigint" => "bigint",
        "smallint" => "smallint",
        "tinyint" => "smallint",
        "bit" => "boolean",
        "decimal" or "numeric" => "numeric",
        "money" or "smallmoney" => "numeric(19,4)",
        "float" => "double precision",
        "real" => "real",
        "datetime" or "datetime2" => "timestamp",
        "smalldatetime" => "timestamp",
        "date" => "date",
        "time" => "time",
        "datetimeoffset" => "timestamptz",
        "char" => "char",
        "nchar" => "char",
        "varchar" => "varchar",
        "nvarchar" => "varchar",
        "text" => "text",
        "ntext" => "text",
        "binary" or "varbinary" => "bytea",
        "image" => "bytea",
        "uniqueidentifier" => "uuid",
        "xml" => "xml",
        "json" => "jsonb",
        _ => "text"
    };

    private string MapReferentialAction(string action) => action switch
    {
        "CASCADE" => "CASCADE",
        "SET_NULL" => "SET NULL",
        "SET_DEFAULT" => "SET DEFAULT",
        "NO_ACTION" => "NO ACTION",
        _ => "NO ACTION"
    };

    private Dictionary<string, string> GetTypeMappings()
    {
        return new Dictionary<string, string>
        {
            // Integer types
            ["int"] = "integer",
            ["bigint"] = "bigint",
            ["smallint"] = "smallint",
            ["tinyint"] = "smallint",
            // Boolean
            ["bit"] = "boolean",
            // Exact numeric
            ["decimal"] = "numeric",
            ["numeric"] = "numeric",
            ["money"] = "numeric(19,4)",
            ["smallmoney"] = "numeric(10,4)",
            // Approximate numeric
            ["float"] = "double precision",
            ["real"] = "real",
            // Date / time
            ["datetime"] = "timestamp",
            ["datetime2"] = "timestamp",
            ["smalldatetime"] = "timestamp",
            ["date"] = "date",
            ["time"] = "time",
            ["datetimeoffset"] = "timestamptz",
            // Character types
            ["char"] = "char",
            ["nchar"] = "char",
            ["varchar"] = "varchar",
            ["nvarchar"] = "varchar",
            ["text"] = "text",
            ["ntext"] = "text",
            // Binary / blob
            ["binary"] = "bytea",
            ["varbinary"] = "bytea",
            ["image"] = "bytea",
            ["rowversion"] = "bytea",
            ["timestamp"] = "bytea", // SQL Server 'timestamp' is rowversion

            // GUID / UUID
            ["uniqueidentifier"] = "uuid",

            // Document / special types
            ["xml"] = "xml",
            ["json"] = "jsonb",
            ["sql_variant"] = "text",
            ["hierarchyid"] = "text",

            // Spatial (map to bytea by default; consider PostGIS types if available)
            ["geometry"] = "bytea",
            ["geography"] = "bytea"
        };
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}
