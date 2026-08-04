using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public sealed class CoreApplicationStatusPort : IApplicationStatusPort
{
    private readonly CodexSyncService _syncService;

    public CoreApplicationStatusPort()
        : this(new CodexSyncService())
    {
    }

    public CoreApplicationStatusPort(CodexSyncService syncService)
    {
        _syncService = syncService ?? throw new ArgumentNullException(nameof(syncService));
    }

    public async Task<StatusSnapshot> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        StatusSnapshot status = await _syncService.GetStatusAsync(
            request.CodexHome,
            NormalizeOptional(request.SqliteHomeOverride));
        cancellationToken.ThrowIfCancellationRequested();
        return status;
    }

    private static string? NormalizeOptional(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
