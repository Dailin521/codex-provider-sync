using CodexProviderSync.Core;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class StatusCoordinationTests
{
    [Fact]
    public async Task ActiveHomeLock_ReturnsLastCompleteSnapshotWithOperationMarker()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"apigather\"");
        await fixture.WriteStateDbAsync([("thread-home-status", "apigather", false)]);
        CodexSyncService service = new();
        StatusSnapshot baseline = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal("apigather", baseline.CurrentProvider.Provider);

        await using (LockHandle held = await new LockService().AcquireLockAsync(
            fixture.CodexHome,
            "home-status-fixture"))
        {
            await fixture.WriteConfigAsync("model_provider = \"openai\"");

            StatusSnapshot blocked = await new CodexSyncService().GetStatusAsync(fixture.CodexHome);

            Assert.Equal("apigather", blocked.CurrentProvider.Provider);
            Assert.Equal(baseline.StorageRevision, blocked.StorageRevision);
            Assert.Equal("codex-home", blocked.OperationInProgress!.BusyScope);
            Assert.Equal("home-status-fixture", blocked.OperationInProgress.Operation);
            Assert.Equal("active", blocked.OperationInProgress.LockState);
            Assert.Equal("codex-home-lock", blocked.StatusReadBlocked!.Reason);
            Assert.Contains("last complete snapshot", TextFormatter.FormatStatus(blocked));
        }

        StatusSnapshot refreshed = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal("openai", refreshed.CurrentProvider.Provider);
        Assert.Null(refreshed.OperationInProgress);
    }

    [Fact]
    public async Task ActiveSharedStateDbLock_DoesNotExposeIntermediateSqliteRows()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([("thread-state-status", "relay", false)]);
        CodexSyncService service = new();
        StatusSnapshot baseline = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal(1, baseline.SqliteCounts!.Sessions["relay"]);
        baseline.SqliteCounts.Sessions["relay"] = 999;
        baseline.RolloutCounts.Sessions["caller-poison"] = 999;

        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(fixture.StateDbPath());
        await using (LockHandle held = await new LockService().AcquireStateDbLockAsync(
            resource,
            "state-status-fixture"))
        {
            await using SqliteConnection connection = fixture.OpenSqliteConnection();
            await connection.OpenAsync();
            SqliteCommand update = connection.CreateCommand();
            update.CommandText = "UPDATE threads SET model_provider = 'external' WHERE id = 'thread-state-status'";
            Assert.Equal(1, await update.ExecuteNonQueryAsync());

            StatusSnapshot blocked = await new CodexSyncService().GetStatusAsync(fixture.CodexHome);

            Assert.Equal(1, blocked.SqliteCounts!.Sessions["relay"]);
            Assert.False(blocked.SqliteCounts.Sessions.ContainsKey("external"));
            Assert.False(blocked.RolloutCounts.Sessions.ContainsKey("caller-poison"));
            Assert.Equal("state-db", blocked.OperationInProgress!.BusyScope);
            Assert.Equal("state-status-fixture", blocked.OperationInProgress.Operation);
            Assert.Equal(resource.ResourceKey, (await new LockService()
                .InspectStateDbLockAsync(resource)).ResourceKey);
        }

        StatusSnapshot refreshed = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal(1, refreshed.SqliteCounts!.Sessions["external"]);
        Assert.Null(refreshed.OperationInProgress);
    }

    [Fact]
    public async Task UnverifiableHomeLock_ReturnsAnExplicitlyIncompleteSnapshotWithoutMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string lockPath = AppConstants.LockPath(fixture.CodexHome);
        Directory.CreateDirectory(lockPath);
        string ownerPath = Path.Combine(lockPath, "owner.json");
        await File.WriteAllTextAsync(ownerPath, "malformed-owner");

        StatusSnapshot status = await new CodexSyncService().GetStatusAsync(fixture.CodexHome);

        Assert.Equal("unknown", status.CurrentProvider.Provider);
        Assert.False(status.RolloutScanComplete);
        Assert.Equal("unverifiable", status.OperationInProgress!.LockState);
        Assert.Equal(LockService.LockUnverifiableErrorCode, status.OperationInProgress.ErrorCode);
        Assert.Equal("malformed-owner", await File.ReadAllTextAsync(ownerPath));
        Assert.True(Directory.Exists(lockPath));
    }
}
