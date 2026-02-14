using Microsoft.AspNetCore.SignalR;
using System.Text.RegularExpressions;

namespace SqlToPostgresMigrationUI.Services.SignalR;

public class MigrationHub : Hub
{
    private readonly ILogger<MigrationHub> _logger;

    public MigrationHub(ILogger<MigrationHub> logger)
    {
        _logger = logger;
    }

    public async Task SubscribeToMigration(string migrationId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, $"migration-{migrationId}");
        _logger.LogDebug("Client {ConnectionId} subscribed to {MigrationId}",
            Context.ConnectionId, migrationId);
    }

    public async Task UnsubscribeFromMigration(string migrationId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, $"migration-{migrationId}");
    }

    public override async Task OnConnectedAsync()
    {
        _logger.LogDebug("Client connected: {ConnectionId}", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogDebug("Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }
}
