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

    private async Task<NpgsqlConnection> CreateOpenConnectionAsync(CancellationToken cancellationToken)
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        return conn;
    }

    public async Task<NpgsqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await CreateOpenConnectionAsync(cancellationToken);
        return await connection.BeginTransactionAsync(cancellationToken);
    }

    public async Task EnsureDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await CreateOpenConnectionAsync(cancellationToken);
        // Create extensions if needed
        var extensions = new[] { "uuid-ossp", "pgcrypto" };
        foreach (var ext in extensions)
        {
            try
            {
                await using var cmd = new NpgsqlCommand($"CREATE EXTENSION IF NOT EXISTS \"{ext}\"", connection);
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
        bool deferNotNullConstraints = false,
        CancellationToken cancellationToken = default)
    {
        if (dryRun)
        {
            _logger.LogInformation("[DRY RUN] Would create table: {Table}", table.TargetName);
            return;
        }

        await using var connection = await CreateOpenConnectionAsync(cancellationToken);

        // Drop table if exists (with CASCADE to handle dependencies)
        var dropSql = $@"
            DROP TABLE IF EXISTS ""{table.TargetSchema}"".""{table.TargetName}"" CASCADE;";

        await using var dropCmd = new NpgsqlCommand(dropSql, connection);
        await dropCmd.ExecuteNonQueryAsync(cancellationToken);

        // Build CREATE TABLE statement
        var createSql = BuildCreateTableSql(table, deferNotNullConstraints);

        _logger.LogDebug("Creating table {Table} with SQL: {Sql}", table.TargetName, createSql);

        await using var createCmd = new NpgsqlCommand(createSql, connection);
        await createCmd.ExecuteNonQueryAsync(cancellationToken);

        // Create sequences for identity columns
        await CreateSequencesAsync(table, connection, cancellationToken);

        // Create indexes
        await CreateIndexesAsync(table, connection, cancellationToken);
    }

    private string BuildCreateTableSql(TableSchema table, bool deferNotNullConstraints = false)
    {
        var sb = new StringBuilder();
        sb.AppendLine($@"CREATE TABLE ""{table.TargetSchema}"".""{table.TargetName}"" (");

        var columnDefinitions = new List<string>();
        var pkColumns = table.PrimaryKey?.Columns?.ToHashSet() ?? new HashSet<string>();

        foreach (var column in table.Columns.OrderBy(c => c.OrdinalPosition))
        {
            bool isPK = pkColumns.Contains(column.Name);
            var colDef = BuildColumnDefinition(column, deferNotNullConstraints, isPK);
            columnDefinitions.Add(colDef);
        }

        // Add primary key constraint
        if (table.PrimaryKey != null && table.PrimaryKey.Columns.Any())
        {
            var pkColumnsStr = string.Join(", ", table.PrimaryKey.Columns.Select(c => $"\"{c}\""));
            columnDefinitions.Add($"CONSTRAINT \"{table.PrimaryKey.Name}\" PRIMARY KEY ({pkColumnsStr})");
        }

        sb.AppendLine(string.Join(",\n", columnDefinitions));
        sb.AppendLine(");");

        return sb.ToString();
    }

    private string BuildColumnDefinition(ColumnSchema column, bool deferNotNullConstraints = false, bool isPrimaryKey = false)
    {
        var sb = new StringBuilder();
        // Handle varchar/char with length only if not already present
        if ((column.TargetType == "varchar" || column.TargetType == "char") && column.MaxLength.HasValue && column.MaxLength > 0)
        {
            sb.Append($"\"{column.Name}\" {column.TargetType}({column.MaxLength})");
        }
        else if (column.Precision.HasValue && column.Scale.HasValue && column.TargetType == "numeric")
        {
            sb.Append($"\"{column.Name}\" {column.TargetType}({column.Precision}, {column.Scale})");
        }
        else
        {
            sb.Append($"\"{column.Name}\" {column.TargetType}");
        }

        // Add nullability
        if (deferNotNullConstraints)
            sb.Append(isPrimaryKey ? " NOT NULL" : " NULL");
        else
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

        // Remove surrounding parentheses (SQL Server wraps defaults in them)
        defaultValue = defaultValue.Trim();
        while (defaultValue.StartsWith("(") && defaultValue.EndsWith(")"))
            defaultValue = defaultValue.Substring(1, defaultValue.Length - 2).Trim();

        // Handle SQL Server specific defaults
        if (defaultValue.Equals("getdate()", StringComparison.OrdinalIgnoreCase))
            return "CURRENT_TIMESTAMP";
        if (defaultValue.Equals("newid()", StringComparison.OrdinalIgnoreCase))
            return "gen_random_uuid()";
        if (defaultValue.Equals("newsequentialid()", StringComparison.OrdinalIgnoreCase))
            return "gen_random_uuid()"; // No direct equivalent

        // If it's a string literal (not already quoted), quote it
        if (column.TargetType.Contains("char") || column.TargetType.Contains("text") || column.TargetType.Contains("varchar"))
        {
            // Remove N prefix for Unicode strings
            if (defaultValue.StartsWith("N'", StringComparison.Ordinal) && defaultValue.EndsWith("'", StringComparison.Ordinal))
                defaultValue = defaultValue.Substring(2, defaultValue.Length - 3);
            else if (defaultValue.StartsWith("'", StringComparison.Ordinal) && defaultValue.EndsWith("'", StringComparison.Ordinal))
                defaultValue = defaultValue.Substring(1, defaultValue.Length - 2);

            // Escape single quotes for PostgreSQL
            defaultValue = defaultValue.Replace("'", "''");
            return $"'{defaultValue}'";
        }

        // For numbers and other literals, return as-is
        return defaultValue;
    }

    private async Task CreateSequencesAsync(TableSchema table, NpgsqlConnection connection, CancellationToken cancellationToken)
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

            await using var cmd = new NpgsqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task CreateIndexesAsync(TableSchema table, NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        foreach (var index in table.Indexes)
        {
            var columns = string.Join(", ", index.Columns.Select(c => $@"""{c}"""));
            var unique = index.IsUnique ? "UNIQUE " : "";

            var sql = $@"
                CREATE {unique}INDEX IF NOT EXISTS ""{index.Name}""
                ON ""{table.TargetSchema}"".""{table.TargetName}"" ({columns});";

            await using var cmd = new NpgsqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<long> BulkInsertAsync(
        TableSchema table,
        IAsyncEnumerable<object[]> rows,
        CancellationToken cancellationToken = default)
    {
        var rowCount = 0L;
        await using var connection = await CreateOpenConnectionAsync(cancellationToken);
        try
        {
            // Validate column count upfront
            var expectedColumnCount = table.Columns.Count;
            if (expectedColumnCount == 0)
            {
                throw new InvalidOperationException($"Table {table.TargetName} has no columns defined");
            }

            // Log table schema for debugging
            _logger.LogDebug("Starting bulk insert for table {Table} with {ColumnCount} columns",
                table.TargetName, expectedColumnCount);
            foreach (var col in table.Columns.OrderBy(c => c.OrdinalPosition))
            {
                _logger.LogDebug("  Column {Position}: {Name} ({SourceType} -> {TargetType}) Nullable:{IsNullable}",
                    col.OrdinalPosition, col.Name, col.SourceType, col.TargetType, col.IsNullable);
            }

            // Use Binary COPY for maximum performance
            await using var writer = await connection.BeginBinaryImportAsync(
                $@"COPY ""{table.TargetSchema}"".""{table.TargetName}"" ({GetColumnList(table)}) FROM STDIN (FORMAT BINARY)",
                cancellationToken);

            var notNullColumns = table.Columns.Where(c => !c.IsNullable).ToList();

            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                // Validate row has correct column count
                if (row.Length != expectedColumnCount)
                {
                    _logger.LogError(
                        "Row {RowNumber} has {RowLength} columns but table {Table} expects {ExpectedCount}",
                        rowCount + 1, row.Length, table.TargetName, expectedColumnCount);
                    throw new InvalidOperationException(
                        $"Row {rowCount + 1} data mismatch: expected {expectedColumnCount} columns, got {row.Length}");
                }

                // Validate NOT NULL constraints before writing
                for (int i = 0; i < row.Length; i++)
                {
                    var column = table.Columns[i];
                    var value = row[i];

                    if ((value == null || value == DBNull.Value) && !column.IsNullable)
                    {
                        _logger.LogError(
                            "Row {RowNumber}: Column {ColumnName} (position {ColumnPosition}) has NULL value but is marked NOT NULL in schema. " +
                            "Source type: {SourceType}, Target type: {TargetType}",
                            rowCount + 1, column.Name, i + 1, column.SourceType, column.TargetType);
                        throw new InvalidOperationException(
                            $"Data constraint violation at row {rowCount + 1}: Column '{column.Name}' cannot be NULL");
                    }
                    // Additional check: treat empty string as violation for NOT NULL text columns
                    else if (!column.IsNullable && (column.TargetType == "text" || column.TargetType == "varchar" || column.TargetType == "character varying"))
                    {
                        if (value is string s && s == "")
                        {
                            _logger.LogError(
                                "Row {RowNumber}: Column {ColumnName} (position {ColumnPosition}) has empty string value but is marked NOT NULL in schema. " +
                                "Source type: {SourceType}, Target type: {TargetType}",
                                rowCount + 1, column.Name, i + 1, column.SourceType, column.TargetType);
                            throw new InvalidOperationException(
                                $"Data constraint violation at row {rowCount + 1}: Column '{column.Name}' cannot be empty");
                        }
                    }
                }

                await writer.StartRowAsync(cancellationToken);

                try
                {
                    for (int i = 0; i < row.Length; i++)
                    {
                        var column = table.Columns[i];
                        var value = row[i];

                        if (value == null || value == DBNull.Value)
                        {
                            await writer.WriteNullAsync(cancellationToken);
                        }
                        else
                        {
                            await WriteValueAsync(writer, value, column, cancellationToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error writing row {RowNumber} to table {Table}. Row had {ColumnCount} columns",
                        rowCount + 1, table.TargetName, row.Length);
                    throw;
                }

                rowCount++;

                // Log progress every 10k rows
                if (rowCount % 10000 == 0)
                {
                    _logger.LogInformation("Inserted {RowCount} rows into {Table}", rowCount, table.TargetName);
                }
            }

            _logger.LogDebug("Completing binary import for table {Table}...", table.TargetName);
            await writer.CompleteAsync(cancellationToken);
            _logger.LogInformation("Successfully inserted {RowCount} rows into {Table}", rowCount, table.TargetName);
            return rowCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "BulkInsertAsync failed for table {Table} after {RowCount} rows",
                table.TargetName, rowCount);
            throw;
        }
    }

    private async Task WriteValueAsync(
        NpgsqlBinaryImporter writer,
        object value,
        ColumnSchema column,
        CancellationToken cancellationToken)
    {
        try
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
                        await writer.WriteAsync(dateTimeValue.Date, NpgsqlDbType.Date, cancellationToken);
                    else
                        await writer.WriteAsync(dateTimeValue, NpgsqlDbType.Timestamp, cancellationToken);
                    break;
                case DateTimeOffset dtoValue:
                    await writer.WriteAsync(dtoValue, NpgsqlDbType.TimestampTz, cancellationToken);
                    break;
                case string stringValue:
                    // Only write NULL when the source value is actually null; keep empty strings as empty strings for text/varchar columns.
                    if (stringValue == null)
                    {
                        await writer.WriteNullAsync(cancellationToken);
                    }
                    else if (column.TargetType == "jsonb" || column.TargetType == "json")
                    {
                        // For JSON columns, empty string is not valid JSON; treat empty as NULL (optional)
                        if (string.IsNullOrWhiteSpace(stringValue))
                            await writer.WriteNullAsync(cancellationToken);
                        else
                            await writer.WriteAsync(stringValue, NpgsqlDbType.Jsonb, cancellationToken);
                    }
                    else
                    {
                        // Preserve empty string values for text/varchar columns so NOT NULL constraints are not violated.
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
                    await WriteConvertedValueAsync(writer, value, column, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error writing value of type {ValueType} to column {ColumnName} ({TargetType}). Value: {Value}",
                value?.GetType().Name ?? "null", column.Name, column.TargetType, value?.ToString() ?? "null");
            throw;
        }
    }

    private async Task WriteConvertedValueAsync(
        NpgsqlBinaryImporter writer,
        object value,
        ColumnSchema column,
        CancellationToken cancellationToken)
    {
        try
        {
            switch (column.TargetType)
            {
                case "integer":
                case "int4":
                    var intVal = Convert.ToInt32(value);
                    await writer.WriteAsync(intVal, NpgsqlDbType.Integer, cancellationToken);
                    break;
                case "bigint":
                case "int8":
                    var longVal = Convert.ToInt64(value);
                    await writer.WriteAsync(longVal, NpgsqlDbType.Bigint, cancellationToken);
                    break;
                case "smallint":
                case "int2":
                    var shortVal = Convert.ToInt16(value);
                    await writer.WriteAsync(shortVal, NpgsqlDbType.Smallint, cancellationToken);
                    break;
                case "numeric":
                case "decimal":
                    var decimalVal = Convert.ToDecimal(value);
                    await writer.WriteAsync(decimalVal, NpgsqlDbType.Numeric, cancellationToken);
                    break;
                case "double precision":
                case "float8":
                    var doubleVal = Convert.ToDouble(value);
                    await writer.WriteAsync(doubleVal, NpgsqlDbType.Double, cancellationToken);
                    break;
                case "real":
                case "float4":
                    var floatVal = Convert.ToSingle(value);
                    await writer.WriteAsync(floatVal, NpgsqlDbType.Real, cancellationToken);
                    break;
                case "boolean":
                case "bool":
                    var boolVal = Convert.ToBoolean(value);
                    await writer.WriteAsync(boolVal, NpgsqlDbType.Boolean, cancellationToken);
                    break;
                case "text":
                case "varchar":
                case "character varying":
                    // preserve empty strings; only treat null as NULL
                    var stringVal = value?.ToString();
                    if (stringVal == null)
                        await writer.WriteNullAsync(cancellationToken);
                    else
                        await writer.WriteAsync(stringVal, NpgsqlDbType.Text, cancellationToken);
                    break;
                case "date":
                    if (value is DateTime dt)
                        await writer.WriteAsync(dt.Date, NpgsqlDbType.Date, cancellationToken);
                    else
                        await writer.WriteAsync(DateTime.Parse(value.ToString()), NpgsqlDbType.Date, cancellationToken);
                    break;
                case "timestamp":
                case "timestamp without time zone":
                    if (value is DateTime dtNoTz)
                        await writer.WriteAsync(dtNoTz, NpgsqlDbType.Timestamp, cancellationToken);
                    else if (value is DateTimeOffset dtoNoTz)
                        await writer.WriteAsync(dtoNoTz.DateTime, NpgsqlDbType.Timestamp, cancellationToken);
                    else
                        await writer.WriteAsync(DateTime.Parse(value.ToString()), NpgsqlDbType.Timestamp, cancellationToken);
                    break;
                case "timestamp with time zone":
                case "timestamptz":
                    if (value is DateTimeOffset dtoTz)
                        await writer.WriteAsync(dtoTz, NpgsqlDbType.TimestampTz, cancellationToken);
                    else if (value is DateTime dtTz)
                        await writer.WriteAsync(new DateTimeOffset(dtTz), NpgsqlDbType.TimestampTz, cancellationToken);
                    else
                        await writer.WriteAsync(DateTimeOffset.Parse(value.ToString()), NpgsqlDbType.TimestampTz, cancellationToken);
                    break;
                case "jsonb":
                case "json":
                    var jsonStr = value?.ToString();
                    if (string.IsNullOrWhiteSpace(jsonStr))
                        await writer.WriteNullAsync(cancellationToken);
                    else
                        await writer.WriteAsync(jsonStr, NpgsqlDbType.Jsonb, cancellationToken);
                    break;
                case "bytea":
                    if (value is byte[] bytes)
                        await writer.WriteAsync(bytes, NpgsqlDbType.Bytea, cancellationToken);
                    else if (value is string hexStr)
                        await writer.WriteAsync(Convert.FromHexString(hexStr), NpgsqlDbType.Bytea, cancellationToken);
                    else
                        throw new InvalidOperationException($"Cannot convert {value.GetType().Name} to bytea");
                    break;
                case "uuid":
                case "uuid[]":
                    if (value is Guid guid)
                        await writer.WriteAsync(guid, NpgsqlDbType.Uuid, cancellationToken);
                    else if (value is string guidStr && Guid.TryParse(guidStr, out var parsedGuid))
                        await writer.WriteAsync(parsedGuid, NpgsqlDbType.Uuid, cancellationToken);
                    else
                        throw new InvalidOperationException($"Cannot convert {value.GetType().Name} to uuid");
                    break;
                default:
                    var fallbackStr = value?.ToString();
                    if (fallbackStr == null)
                        await writer.WriteNullAsync(cancellationToken);
                    else
                        await writer.WriteAsync(fallbackStr, NpgsqlDbType.Text, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to convert value {Value} (type {ValueType}) for column {ColumnName} (target type {TargetType})",
                value?.ToString() ?? "null", value?.GetType().Name ?? "null", column.Name, column.TargetType);
            throw;
        }
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
                await using var connection = await CreateOpenConnectionAsync(cancellationToken);
                await using var cmd = new NpgsqlCommand(sql, connection);
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
        await using var connection = await CreateOpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, connection);
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

    public void Dispose()
    {
        // No longer need to dispose a shared connection
    }

    public async Task<List<string>> ValidateSchemaCompatibilityAsync(
        TableSchema table,
        IAsyncEnumerable<object[]> rows,
        CancellationToken cancellationToken = default)
    {
        var issues = new List<string>();
        var notNullColumns = table.Columns.Where(c => !c.IsNullable).ToList();

        if (notNullColumns.Count == 0)
            return issues; // No NOT NULL columns to validate

        var rowCount = 0;
        var nullViolations = new Dictionary<string, int>(); // columnName -> count of nulls
        var emptyStringViolations = new Dictionary<string, int>(); // columnName -> count of empty strings

        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            if (rowCount > 10000) // Sample first 10k rows for validation
                break;

            for (int i = 0; i < row.Length && i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                var value = row[i];

                if ((value == null || value == DBNull.Value) && !column.IsNullable)
                {
                    if (!nullViolations.ContainsKey(column.Name))
                        nullViolations[column.Name] = 0;
                    nullViolations[column.Name]++;
                }
                // Additional check: treat empty string as violation for NOT NULL text columns
                else if (!column.IsNullable && (column.TargetType == "text" || column.TargetType == "varchar" || column.TargetType == "character varying"))
                {
                    if (value is string s && s == "")
                    {
                        if (!emptyStringViolations.ContainsKey(column.Name))
                            emptyStringViolations[column.Name] = 0;
                        emptyStringViolations[column.Name]++;
                    }
                }
            }

            rowCount++;
        }

        // Report findings
        foreach (var violation in nullViolations)
        {
            var issue = $"Column '{violation.Key}' has {violation.Value} NULL values in first {rowCount} rows but is marked as NOT NULL in target schema";
            issues.Add(issue);
            _logger.LogWarning(issue);
        }
        foreach (var violation in emptyStringViolations)
        {
            var issue = $"Column '{violation.Key}' has {violation.Value} EMPTY STRING values in first {rowCount} rows but is marked as NOT NULL in target schema";
            issues.Add(issue);
            _logger.LogWarning(issue);
        }

        return issues;
    }

    private string GetColumnList(TableSchema table)
    {
        return string.Join(", ", table.Columns.Select(c => $"\"{c.Name}\""));
    }

    public async Task ApplyNotNullConstraintsAsync(TableSchema table, CancellationToken cancellationToken = default)
    {
        await using var connection = await CreateOpenConnectionAsync(cancellationToken);
        foreach (var column in table.Columns.Where(c => !c.IsNullable))
        {
            var sql = $@"ALTER TABLE ""{table.TargetSchema}"".""{table.TargetName}"" ALTER COLUMN ""{column.Name}"" SET NOT NULL;";
            await using var cmd = new NpgsqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}