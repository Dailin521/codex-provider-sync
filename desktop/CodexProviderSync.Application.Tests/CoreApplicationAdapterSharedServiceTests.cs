using CodexProviderSync.Core;

namespace CodexProviderSync.Application.Tests;

public sealed class CoreApplicationAdapterSharedServiceTests
{
    [Fact]
    public async Task Refresh_UsesSharedApplicationStatus_AndPreservesSettingsProviderMerge()
    {
        string root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-controller-shared-{Guid.NewGuid():N}");
        string codexHome = Path.Combine(root, "codex-home");
        string sqliteHome = Path.Combine(root, "sqlite-home");
        string settingsPath = Path.Combine(root, "appdata", "settings.json");
        Directory.CreateDirectory(codexHome);
        Directory.CreateDirectory(sqliteHome);
        try
        {
            SettingsService settings = new(settingsPath);
            await settings.SaveAsync(new AppSettings
            {
                ManualProviders = ["manual"],
                SavedProviders = ["manual"]
            });
            StatusSnapshot status = CreateStatus(codexHome, sqliteHome);
            RecordingStatusApplicationService shared = new(status);
            CoreApplicationAdapter adapter = new(
                new CodexSyncService(),
                settings,
                new CodexHomeService(),
                shared);

            CoreRefreshState refreshed = await adapter.RefreshAsync(
                new CoreRefreshRequest(codexHome, sqliteHome, "relay"));

            Assert.Equal(1, shared.StatusCalls);
            Assert.Equal(codexHome, shared.LastStatusRequest!.CodexHome);
            Assert.Equal(sqliteHome, shared.LastStatusRequest.SqliteHomeOverride);
            Assert.Same(status, refreshed.Status);
            Assert.Contains(refreshed.Providers, option => option.Id == "relay");
            Assert.Contains(refreshed.Providers, option => option.Id == "manual" && option.IsManual);
            AppSettings persisted = await settings.LoadAsync();
            Assert.Contains("relay", persisted.SavedProviders);
            Assert.Equal("relay", persisted.LastSelectedProvider);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static StatusSnapshot CreateStatus(string codexHome, string sqliteHome) => new()
    {
        CodexHome = codexHome,
        SqliteHome = sqliteHome,
        SqliteHomeSource = "gui",
        CurrentProvider = new CurrentProviderInfo("openai", false),
        ConfiguredProviders = ["openai", "relay"],
        RolloutCounts = new ProviderCounts(),
        LockedRolloutFiles = [],
        UnreadableRolloutFiles = [],
        EncryptedContentCounts = new ProviderCounts(),
        SqliteCounts = null,
        BackupRoot = Path.Combine(codexHome, "backups_state", "provider-sync"),
        BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
    };

    private sealed class RecordingStatusApplicationService(StatusSnapshot status) : IApplicationService
    {
        internal int StatusCalls { get; private set; }

        internal ApplicationStatusRequest? LastStatusRequest { get; private set; }

        public Task<ApplicationOutcome<StatusSnapshot>> GetStatusAsync(
            ApplicationStatusRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            StatusCalls++;
            LastStatusRequest = request;
            DateTimeOffset now = DateTimeOffset.UtcNow;
            return Task.FromResult(new ApplicationOutcome<StatusSnapshot>(
                $"status-{StatusCalls}",
                ApplicationOperationKind.Status,
                ApplicationOperationLifecycle.Succeeded,
                now,
                now,
                status,
                [],
                [],
                [new ApplicationLifecycleEvent(ApplicationOperationLifecycle.Succeeded, now)]));
        }

        public Task<ApplicationOutcome<ApplicationDescription>> DescribeAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationOperationPlan>> CreatePlanAsync(CreateApplicationPlanRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SyncAsync(SyncApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SwitchAsync(SwitchApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<RestoreResult>>> RestoreAsync(RestoreApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>>> PruneAsync(PruneApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
