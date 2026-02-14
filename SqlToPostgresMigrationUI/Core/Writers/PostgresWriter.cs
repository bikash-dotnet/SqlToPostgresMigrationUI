using Npgsql;
using NpgsqlTypes;
using SqlToPostgresMigrationUI.Core.Models;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SqlToPostgresMigrationUI.Core.Writers;

public class PostgresWriter : IDisposable
{
    private readonly string _connectionString;
    private NpgsqlConnection? _connection;
    private readonly ILogger<PostgresWriter> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    public PostgresWriter(string connectionString, ILogger<PostgresWriter> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public async Task<NpgsqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectionAsync(cancellationToken);
        return await _connection!.BeginTransactionAsync(cancellationToken);
    }

    public async Task EnsureDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectionAsync(cancellationToken);

        // Create extensions if needed
        var extensions = new[] { "uuid-ossp", "pgcrypto" };
        foreach (var ext in extensions)
        {
            try
            {
                await using var cmd = new NpgsqlCommand($"CREATE EXTENSION IF NOT EXISTS \"{ext}\"", _connection);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to create extension {Extension}", ext);
            }
        }
    }

    public async Task CreateTableAsync(
        TableSchema table,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        if (dryRun)
        {
            _logger.LogInformation("[DRY RUN] Would create table: {Table}", table.TargetName);
            return;
        }

        await EnsureConnectionAsync(cancellationToken);

        // Drop table if exists (with CASCADE to handle dependencies)
        var dropSql = $@"
            DROP TABLE IF EXISTS ""{table.TargetSchema}"".""{table.TargetName}"" CASCADE;";

        await using var dropCmd = new NpgsqlCommand(dropSql, _connection);
        await dropCmd.ExecuteNonQueryAsync(cancellationToken);

        // Build CREATE TABLE statement
        var createSql = BuildCreateTableSql(table);

        _logger.LogDebug("Creating table {Table} with SQL: {Sql}", table.TargetName, createSql);

        await using var createCmd = new NpgsqlCommand(createSql, _connection);
        await createCmd.ExecuteNonQueryAsync(cancellationToken);

        // Create sequences for identity columns
        await CreateSequencesAsync(table, cancellationToken);

        // Create indexes
        await CreateIndexesAsync(table, cancellationToken);
    }

    private string BuildCreateTableSql(TableSchema table)
    {
        var sb = new StringBuilder();
        sb.AppendLine($@"CREATE TABLE ""{table.TargetSchema}"".""{table.TargetName}"" (");

        var columnDefinitions = new List<string>();

        foreach (var column in table.Columns.OrderBy(c => c.OrdinalPosition))
        {
            var colDef = BuildColumnDefinition(column);
            columnDefinitions.Add(colDef);
        }

        // Add primary key constraint
        if (table.PrimaryKey != null && table.PrimaryKey.Columns.Any())
        {
            var pkColumns = string.Join(", ", table.PrimaryKey.Columns.Select(c => $@"""{c}"""));
            columnDefinitions.Add($@"CONSTRAINT ""{table.PrimaryKey.Name}"" PRIMARY KEY ({pkColumns})");
        }

        sb.AppendLine(string.Join(",\n", columnDefinitions));
        sb.AppendLine(");");

        return sb.ToString();
    }

    private string BuildColumnDefinition(ColumnSchema column)
    {
        var sb = new StringBuilder();
        sb.Append($@"""{column.Name}"" {column.TargetType}");

        // Add length/precision if applicable
        if (column.MaxLength.HasValue && column.MaxLength > 0 &&
            (column.TargetType.Contains("varchar") || column.TargetType.Contains("char")))
        {
            sb.Replace("varchar", $"varchar({column.MaxLength})");
            sb.Replace("char", $"char({column.MaxLength})");
        }
        else if (column.Precision.HasValue && column.Scale.HasValue)
        {
            if (column.TargetType == "numeric")
            {
                sb.Append($"({column.Precision}, {column.Scale})");
            }
        }

        // Add nullability
        sb.Append(column.IsNullable ? " NULL" : " NOT NULL");

        // Add default value
        if (!string.IsNullOrEmpty(column.DefaultValue?.ToString()))
        {
            var defaultValue = FormatDefaultValue(column);
            if (!string.IsNullOrEmpty(defaultValue))
            {
                sb.Append($" DEFAULT {defaultValue}");
            }
        }

        return sb.ToString();
    }

    private string FormatDefaultValue(ColumnSchema column)
    {
        var defaultValue = column.DefaultValue?.ToString() ?? "";

        // Handle SQL Server specific defaults
        if (defaultValue.Contains("getdate()", StringComparison.OrdinalIgnoreCase))
            return "CURRENT_TIMESTAMP";

        if (defaultValue.Contains("newid()", StringComparison.OrdinalIgnoreCase))
            return "gen_random_uuid()";

        if (defaultValue.Contains("newsequentialid()", StringComparison.OrdinalIgnoreCase))
            return "gen_random_uuid()"; // No direct equivalent

        // Handle string defaults
        if (column.TargetType.Contains("varchar") || column.TargetType.Contains("text") ||
            column.TargetType.Contains("char") || column.TargetType.Contains("date") ||
            column.TargetType.Contains("timestamp"))
        {
            return $"'{defaultValue.Trim('\'')}'";
        }

        return defaultValue;
    }

    private async Task CreateSequencesAsync(TableSchema table, CancellationToken cancellationToken)
    {
        var identityColumns = table.Columns.Where(c => c.IsIdentity).ToList();

        foreach (var column in identityColumns)
        {
            var sequenceName = $"{table.TargetName}_{column.Name}_seq";
            var startValue = column.IdentitySeed ?? "1";

            var sql = $@"
                CREATE SEQUENCE IF NOT EXISTS ""{table.TargetSchema}"".""{sequenceName}""
                START WITH {startValue}
                INCREMENT BY {column.IdentityIncrement ?? "1"}
                NO MAXVALUE
                NO CYCLE;
                
                ALTER TABLE ""{table.TargetSchema}"".""{table.TargetName}""
                ALTER COLUMN ""{column.Name}"" SET DEFAULT nextval('""{table.TargetSchema}"".""{sequenceName}""'::regclass);";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task CreateIndexesAsync(TableSchema table, CancellationToken cancellationToken)
    {
        foreach (var index in table.Indexes)
        {
            var columns = string.Join(", ", index.Columns.Select(c => $@"""{c}"""));
            var unique = index.IsUnique ? "UNIQUE " : "";

            var sql = $@"
                CREATE {unique}INDEX IF NOT EXISTS ""{index.Name}""
                ON ""{table.TargetSchema}"".""{table.TargetName}"" ({columns});";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<long> BulkInsertAsync(
        TableSchema table,
        IAsyncEnumerable<object[]> rows,
        CancellationToken cancellationToken = default)
    {
        var rowCount = 0L;

        // Use Binary COPY for maximum performance
        await using var writer = await _connection!.BeginBinaryImportAsync(
            $@"COPY ""{table.TargetSchema}"".""{table.TargetName}"" ({GetColumnList(table)}) FROM STDIN (FORMAT BINARY)",
            cancellationToken);

        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            for (int i = 0; i < row.Length; i++)
            {
                var column = table.Columns[i];
                var value = row[i];

                if (value == DBNull.Value)
                {
                    await writer.WriteNullAsync(cancellationToken);
                }
                else
                {
                    await WriteValueAsync(writer, value, column, cancellationToken);
                }
            }

            await writer.WriteRowAsync(cancellationToken);
            rowCount++;

            // Progress reporting can be added here
        }

        await writer.CompleteAsync(cancellationToken);
        return rowCount;
    }

    private async Task WriteValueAsync(
        NpgsqlBinaryImporter writer,
        object value,
        ColumnSchema column,
        CancellationToken cancellationToken)
    {
        switch (value)
        {
            case int intValue:
                await writer.WriteAsync(intValue, NpgsqlDbType.Integer, cancellationToken);
                break;
            case long longValue:
                await writer.WriteAsync(longValue, NpgsqlDbType.Bigint, cancellationToken);
                break;
            case short shortValue:
                await writer.WriteAsync(shortValue, NpgsqlDbType.Smallint, cancellationToken);
                break;
            case bool boolValue:
                await writer.WriteAsync(boolValue, NpgsqlDbType.Boolean, cancellationToken);
                break;
            case decimal decimalValue:
                await writer.WriteAsync(decimalValue, NpgsqlDbType.Numeric, cancellationToken);
                break;
            case double doubleValue:
                await writer.WriteAsync(doubleValue, NpgsqlDbType.Double, cancellationToken);
                break;
            case float floatValue:
                await writer.WriteAsync(floatValue, NpgsqlDbType.Real, cancellationToken);
                break;
            case DateTime dateTimeValue:
                if (column.TargetType == "date")
                    await writer.WriteAsync(dateTimeValue, NpgsqlDbType.Date, cancellationToken);
                else
                    await writer.WriteAsync(dateTimeValue, NpgsqlDbType.Timestamp, cancellationToken);
                break;
            case DateTimeOffset dtoValue:
                await writer.WriteAsync(dtoValue, NpgsqlDbType.TimestampTz, cancellationToken);
                break;
            case string stringValue:
                if (column.TargetType == "jsonb" || column.TargetType == "json")
                {
                    // Handle JSON columns
                    await writer.WriteAsync(stringValue, NpgsqlDbType.Jsonb, cancellationToken);
                }
                else
                {
                    await writer.WriteAsync(stringValue, NpgsqlDbType.Text, cancellationToken);
                }
                break;
            case byte[] byteArrayValue:
                await writer.WriteAsync(byteArrayValue, NpgsqlDbType.Bytea, cancellationToken);
                break;
            case Guid guidValue:
                await writer.WriteAsync(guidValue, NpgsqlDbType.Uuid, cancellationToken);
                break;
            default:
                // Fallback to string representation
                await writer.WriteAsync(value.ToString(), NpgsqlDbType.Text, cancellationToken);
                break;
        }
    }

    private string GetColumnList(TableSchema table)
    {
        return string.Join(", ", table.Columns.Select(c => $@"""{c.Name}"""));
    }

    public async Task CreateForeignKeysAsync(
        List<ForeignKeySchema> foreignKeys,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        if (dryRun)
        {
            _logger.LogInformation("[DRY RUN] Would create {Count} foreign keys", foreignKeys.Count);
            return;
        }

        foreach (var fk in foreignKeys)
        {
            var sourceColumns = string.Join(", ", fk.SourceColumns.Select(c => $@"""{c}"""));
            var targetColumns = string.Join(", ", fk.TargetColumns.Select(c => $@"""{c}"""));

            var sql = $@"
                ALTER TABLE ""{MapSchema(fk.SourceSchema)}"".""{fk.SourceTable}""
                ADD CONSTRAINT ""{fk.Name}""
                FOREIGN KEY ({sourceColumns})
                REFERENCES ""{MapSchema(fk.TargetSchema)}"".""{fk.TargetTable}"" ({targetColumns})
                ON UPDATE {fk.UpdateRule}
                ON DELETE {fk.DeleteRule};";

            try
            {
                await using var cmd = new NpgsqlCommand(sql, _connection);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to create foreign key {FKName}", fk.Name);
                // Continue with other FKs - some might fail due to data issues
            }
        }
    }

    public async Task<long> GetRowCountAsync(TableSchema table, CancellationToken cancellationToken)
    {
        var sql = $@"SELECT COUNT(*) FROM ""{table.TargetSchema}"".""{table.TargetName}""";
        await using var cmd = new NpgsqlCommand(sql, _connection);
        return (long)(await cmd.ExecuteScalarAsync(cancellationToken) ?? 0);
    }

    public async Task<string> ComputeChecksumAsync(TableSchema table, CancellationToken cancellationToken)
    {
        // Simple row count + column count checksum
        // For production, you might want a more robust checksum
        var rowCount = await GetRowCountAsync(table, cancellationToken);
        var columnCount = table.Columns.Count;

        using var md5 = MD5.Create();
        var input = $"{table.TargetName}:{rowCount}:{columnCount}";
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash);
    }

    private string MapSchema(string sourceSchema) => sourceSchema == "dbo" ? "public" : sourceSchema;

    private async Task EnsureConnectionAsync(CancellationToken cancellationToken)
    {
        _connection ??= new NpgsqlConnection(_connectionString);

        if (_connection.State != ConnectionState.Open)
            await _connection.OpenAsync(cancellationToken);
    }

    public void Dispose()
    {
        _connection?.Dispose();
    }
}