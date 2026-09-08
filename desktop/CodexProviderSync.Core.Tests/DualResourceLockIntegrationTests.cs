using CodexProviderSync.Core;

namespace CodexProviderSync.Core.Tests;

public sealed class DualResourceLockIntegrationTests
{
    [Fact]
    public async Task DifferentCodexHomes_SharingOneStateDb_ContendBeforeTheLosingBackup()
    {
        TestCodexHomeFixture firstFixture = await TestCodexHomeFixture.CreateAsync();
        TestCodexHomeFixture secondFixture = await TestCodexHomeFixture.CreateAsync();
        await firstFixture.WriteConfigAsync("model_provider = \"openai\"");
        await secondFixture.WriteConfigAsync("model_provider = \"openai\"");
        string firstRollout = firstFixture.RolloutPath("sessions", "rollout-shared-a.jsonl");
        string secondRollout = secondFixture.RolloutPath("sessions", "rollout-shared-b.jsonl");
        await firstFixture.WriteRolloutAsync(firstRollout, "thread-shared-a", "custom");
        await secondFixture.WriteRolloutAsync(secondRollout, "thread-shared-b", "custom");
        string sharedSqliteHome = Path.Combine(firstFixture.Root, "shared-sqlite");
        string sharedStateDb = Path.Combine(sharedSqliteHome, AppConstants.DbFileBasename);
        await firstFixture.WriteStateDbAtAsync(
            sharedStateDb,
            [
                ("thread-shared-a", "custom", false),
                ("thread-shared-b", "custom", false)
            ],
            model: null);
        byte[] losingConfigBefore = await File.ReadAllBytesAsync(
            Path.Combine(secondFixture.CodexHome, "config.toml"));
        byte[] losingRolloutBefore = await File.ReadAllBytesAsync(secondRollout);

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CodexSyncService winner = new();
        winner.FaultInjector = async (point, _, _) =>
        {
            if (point == "before_backup")
            {
                entered.TrySetResult();
                await release.Task;
            }
        };
        Task<SyncResult> winnerTask = winner.RunSyncAsync(
            firstFixture.CodexHome,
            provider: "openai",
            explicitSqliteHome: sharedSqliteHome);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        try
        {
            InvalidOperationException busy = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new CodexSyncService().RunSyncAsync(
                    secondFixture.CodexHome,
                    provider: "openai",
                    explicitSqliteHome: sharedSqliteHome));
            Assert.True(LockService.IsOperationBusy(busy));
            Assert.Equal("state-db", busy.Data["codex-provider-sync/lock-scope"]);
            Assert.False(Directory.Exists(AppConstants.DefaultBackupRoot(secondFixture.CodexHome)));
            Assert.Equal(losingConfigBefore, await File.ReadAllBytesAsync(
                Path.Combine(secondFixture.CodexHome, "config.toml")));
            Assert.Equal(losingRolloutBefore, await File.ReadAllBytesAsync(secondRollout));
        }
        finally
        {
            release.TrySetResult();
            await winnerTask;
        }
    }
}
