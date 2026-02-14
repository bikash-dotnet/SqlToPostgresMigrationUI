using SqlToPostgresMigrationUI.Core.Models;
using System.Text.Json;

namespace SqlToPostgresMigrationUI.Services;

public class MigrationStateService
{
    private readonly string _stateDirectory;
    private readonly ILogger<MigrationStateService> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    public MigrationStateService(
        IConfiguration configuration,
        ILogger<MigrationStateService> logger)
    {
        _stateDirectory = configuration["Migration:StateFilePath"] ?? "migration-state";
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions { WriteIndented = true };

        if (!Directory.Exists(_stateDirectory))
        {
            Directory.CreateDirectory(_stateDirectory);
        }
    }

    public async Task SaveMigrationStateAsync(string migrationId, MigrationReport report)
    {
        try
        {
            var filePath = Path.Combine(_stateDirectory, $"{migrationId}.json");
            var json = JsonSerializer.Serialize(report, _jsonOptions);
            await File.WriteAllTextAsync(filePath, json);

            _logger.LogInformation("Saved state for migration {MigrationId}", migrationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save state for migration {MigrationId}", migrationId);
        }
    }

    public async Task<MigrationReport?> LoadMigrationStateAsync(string migrationId)
    {
        try
        {
            var filePath = Path.Combine(_stateDirectory, $"{migrationId}.json");
            if (!File.Exists(filePath))
            {
                return null;
            }

            var json = await File.ReadAllTextAsync(filePath);
            return JsonSerializer.Deserialize<MigrationReport>(json, _jsonOptions);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load state for migration {MigrationId}", migrationId);
            return null;
        }
    }

    public IEnumerable<string> GetCompletedMigrations()
    {
        if (!Directory.Exists(_stateDirectory))
        {
            return Enumerable.Empty<string>();
        }

        return Directory.GetFiles(_stateDirectory, "*.json")
            .Select(Path.GetFileNameWithoutExtension)
            .Where(x => x != null)
            .Select(x => x!);
    }
}
