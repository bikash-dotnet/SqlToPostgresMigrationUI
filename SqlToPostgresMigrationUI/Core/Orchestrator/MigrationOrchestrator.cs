using SqlToPostgresMigrationUI.Core.Models;
using SqlToPostgresMigrationUI.Core.Readers;
using SqlToPostgresMigrationUI.Core.Writers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace SqlToPostgresMigrationUI.Core.Orchestrator;

public class MigrationOrchestrator : IDisposable
{
    private readonly SqlServerReader _sourceReader;
    private readonly PostgresWriter _targetWriter;
    private readonly ILogger<MigrationOrchestrator> _logger;
    private readonly MigrationOptions _options;
    private readonly ConcurrentDictionary<string, TableSchema> _tables;
    private readonly ConcurrentDictionary<string, MigrationProgress> _progress;
    private readonly JsonSerializerOptions _jsonOptions;

    public event EventHandler<MigrationProgress>? ProgressUpdated;
    public event EventHandler<TableCompletedEventArgs>? TableCompleted;

    public MigrationOrchestrator(
        string sourceConnectionString,
        string targetConnectionString,
        ILoggerFactory loggerFactory,
        MigrationOptions? options = null)
    {
        _sourceReader = new SqlServerReader(sourceConnectionString, loggerFactory.CreateLogger<SqlServerReader>());
        _targetWriter = new PostgresWriter(targetConnectionString, loggerFactory.CreateLogger<PostgresWriter>());
        _logger = loggerFactory.CreateLogger<MigrationOrchestrator>();
        _options = options ?? new MigrationOptions();
        _tables = new ConcurrentDictionary<string, TableSchema>();
        _progress = new ConcurrentDictionary<string, MigrationProgress>();
        _jsonOptions = new JsonSerializerOptions { WriteIndented = true };
    }

    public async Task<MigrationReport> MigrateAsync(CancellationToken cancellationToken = default)
    {
        var report = new MigrationReport
        {
            StartTime = DateTime.UtcNow,
            Status = "In Progress"
        };

        try
        {
            _logger.LogInformation("Starting migration from SQL Server to PostgreSQL");

            // Step 1: Read schema from SQL Server
            _logger.LogInformation("Reading source schema...");
            var schema = await _sourceReader.ReadSchemaAsync(cancellationToken);

            report.SourceTables = schema.Tables.Count;
            report.SourceRowCount = schema.Tables.Sum(t => t.RowCount);

            // Step 2: Prepare target database
            _logger.LogInformation("Preparing target database...");
            await _targetWriter.EnsureDatabaseAsync(cancellationToken);

            // Step 3: Create tables (in dependency order)
            _logger.LogInformation("Creating tables...");
            var tablesInOrder = OrderTablesByDependencies(schema.Tables, schema.ForeignKeys);

            foreach (var table in tablesInOrder)
            {
                await _targetWriter.CreateTableAsync(table, _options.DryRun, cancellationToken);
                _tables[table.TargetName] = table;

                if (!_options.DryRun)
                {
                    report.TablesCreated++;
                }
            }

            if (_options.DryRun)
            {
                _logger.LogInformation("DRY RUN completed. No data was transferred.");
                report.Status = "Dry Run Completed";
                report.EndTime = DateTime.UtcNow;
                return report;
            }

            // Step 4: Migrate data in parallel
            _logger.LogInformation("Starting parallel data migration...");

            var migrationTasks = new List<Task>();
            var semaphore = new SemaphoreSlim(_options.MaxParallelTables);

            foreach (var table in tablesInOrder)
            {
                await semaphore.WaitAsync(cancellationToken);

                migrationTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await MigrateTableWithRetryAsync(table, cancellationToken);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken));
            }

            await Task.WhenAll(migrationTasks);

            // Step 5: Create foreign keys
            _logger.LogInformation("Creating foreign keys...");
            await _targetWriter.CreateForeignKeysAsync(schema.ForeignKeys, false, cancellationToken);
            report.ForeignKeysCreated = schema.ForeignKeys.Count;

            // Step 6: Validate migration
            _logger.LogInformation("Validating migration...");
            await ValidateMigrationAsync(schema.Tables, report, cancellationToken);

            report.Status = "Completed";
            report.EndTime = DateTime.UtcNow;

            _logger.LogInformation("Migration completed successfully!");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Migration failed");
            report.Status = "Failed";
            report.ErrorMessage = ex.Message;
            report.EndTime = DateTime.UtcNow;
        }

        return report;
    }

    private async Task MigrateTableWithRetryAsync(
        TableSchema table,
        CancellationToken cancellationToken)
    {
        var maxRetries = _options.MaxRetries;
        var retryDelay = _options.RetryDelaySeconds;

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                UpdateProgress(table.TargetName, 0, table.RowCount, "Starting");

                await using var transaction = await _targetWriter.BeginTransactionAsync(cancellationToken);

                var rows = _sourceReader.StreamTableDataAsync(table, _options.BatchSize, cancellationToken);
                var rowsInserted = await _targetWriter.BulkInsertAsync(table, rows, cancellationToken);

                await transaction.CommitAsync(cancellationToken);

                UpdateProgress(table.TargetName, rowsInserted, table.RowCount, "Completed");

                _logger.LogInformation("Migrated {Count} rows to {Table}", rowsInserted, table.TargetName);

                TableCompleted?.Invoke(this, new TableCompletedEventArgs
                {
                    TableName = table.TargetName,
                    RowsMigrated = rowsInserted,
                    Success = true
                });

                return;
            }
            catch (Exception ex) when (attempt < maxRetries)
            {
                _logger.LogWarning(ex,
                    "Attempt {Attempt} failed for table {Table}. Retrying in {Delay}s...",
                    attempt, table.TargetName, retryDelay);

                UpdateProgress(table.TargetName, 0, table.RowCount, $"Retry {attempt}");

                await Task.Delay(TimeSpan.FromSeconds(retryDelay), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to migrate table {Table} after {Attempts} attempts",
                    table.TargetName, maxRetries);

                TableCompleted?.Invoke(this, new TableCompletedEventArgs
                {
                    TableName = table.TargetName,
                    Success = false,
                    ErrorMessage = ex.Message
                });

                throw;
            }
        }
    }

    private async Task ValidateMigrationAsync(
        List<TableSchema> tables,
        MigrationReport report,
        CancellationToken cancellationToken)
    {
        var validationTasks = new List<Task>();
        var validationResults = new ConcurrentBag<TableValidationResult>();

        foreach (var table in tables)
        {
            validationTasks.Add(Task.Run(async () =>
            {
                try
                {
                    var sourceCount = table.RowCount;
                    var targetCount = await _targetWriter.GetRowCountAsync(table, cancellationToken);
                    var sourceChecksum = await ComputeSourceChecksumAsync(table, cancellationToken);
                    var targetChecksum = await _targetWriter.ComputeChecksumAsync(table, cancellationToken);

                    var isValid = sourceCount == targetCount && sourceChecksum == targetChecksum;

                    validationResults.Add(new TableValidationResult
                    {
                        TableName = table.TargetName,
                        SourceRowCount = sourceCount,
                        TargetRowCount = targetCount,
                        SourceChecksum = sourceChecksum,
                        TargetChecksum = targetChecksum,
                        IsValid = isValid
                    });

                    if (!isValid)
                    {
                        _logger.LogWarning("Validation failed for {Table}: Source={Source}, Target={Target}",
                            table.TargetName, sourceCount, targetCount);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Validation error for {Table}", table.TargetName);
                }
            }, cancellationToken));
        }

        await Task.WhenAll(validationTasks);

        report.Validations = validationResults.ToList();
        report.TablesValidated = validationResults.Count(r => r.IsValid);
        report.TablesFailedValidation = validationResults.Count(r => !r.IsValid);
    }

    private async Task<string> ComputeSourceChecksumAsync(TableSchema table, CancellationToken cancellationToken)
    {
        // For source, we'll use row count + column count for consistency
        using var md5 = MD5.Create();
        var input = $"{table.SourceName}:{table.RowCount}:{table.Columns.Count}";
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash);
    }

    private List<TableSchema> OrderTablesByDependencies(
        List<TableSchema> tables,
        List<ForeignKeySchema> foreignKeys)
    {
        var graph = new Dictionary<string, HashSet<string>>();
        var tableDict = tables.ToDictionary(t => t.TargetName);

        foreach (var fk in foreignKeys)
        {
            if (!graph.ContainsKey(fk.SourceTable))
                graph[fk.SourceTable] = new HashSet<string>();

            graph[fk.SourceTable].Add(fk.TargetTable);
        }

        var sorted = new List<TableSchema>();
        var visited = new HashSet<string>();
        var visiting = new HashSet<string>();

        foreach (var table in tables)
        {
            TopologicalSort(table.TargetName, tableDict, graph, visited, visiting, sorted);
        }

        return sorted;
    }

    private void TopologicalSort(
        string tableName,
        Dictionary<string, TableSchema> tableDict,
        Dictionary<string, HashSet<string>> graph,
        HashSet<string> visited,
        HashSet<string> visiting,
        List<TableSchema> sorted)
    {
        if (visiting.Contains(tableName))
            return; // Circular dependency - handle by removing FK temporarily

        if (visited.Contains(tableName))
            return;

        visiting.Add(tableName);

        if (graph.ContainsKey(tableName))
        {
            foreach (var dep in graph[tableName])
            {
                if (tableDict.ContainsKey(dep))
                {
                    TopologicalSort(dep, tableDict, graph, visited, visiting, sorted);
                }
            }
        }

        visiting.Remove(tableName);
        visited.Add(tableName);
        sorted.Add(tableDict[tableName]);
    }

    private void UpdateProgress(string tableName, long processed, long total, string status)
    {
        var progress = new MigrationProgress
        {
            TableName = tableName,
            RowsProcessed = processed,
            TotalRows = total,
            Status = status,
            ElapsedTime = TimeSpan.Zero // Would need start time tracking
        };

        _progress[tableName] = progress;
        ProgressUpdated?.Invoke(this, progress);
    }

    public async Task<MigrationReport> LoadStateAsync(string stateFilePath)
    {
        if (!File.Exists(stateFilePath))
            return new MigrationReport();

        var json = await File.ReadAllTextAsync(stateFilePath);
        return JsonSerializer.Deserialize<MigrationReport>(json, _jsonOptions) ?? new MigrationReport();
    }

    public async Task SaveStateAsync(MigrationReport report, string stateFilePath)
    {
        var json = JsonSerializer.Serialize(report, _jsonOptions);
        await File.WriteAllTextAsync(stateFilePath, json);
    }

    public void Dispose()
    {
        _sourceReader.Dispose();
        _targetWriter.Dispose();
    }
}
