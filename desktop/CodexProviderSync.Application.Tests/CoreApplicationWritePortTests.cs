using System.Text.Json;
using CodexProviderSync.Core;

namespace CodexProviderSync.Application.Tests;

public sealed class CoreApplicationWritePortTests
{
    [Fact]
    public async Task ProductionSync_DefaultsToDryRunThenAppliesTheExactCorePlan()
    {
        using Fixture fixture = await Fixture.CreateAsync("relay");
        ApplicationService service = CreateService();
        SyncIntent intent = new($"  {fixture.CodexHome}  ", null, "  openai  ", 5);

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> dryRun = await service.SyncAsync(
            new SyncApplicationRequest(intent));

        Assert.Equal(ApplicationOperationLifecycle.ReadyToApply, dryRun.Lifecycle);
        Assert.False(dryRun.Data!.Applied);
        SyncIntent normalizedIntent = Assert.IsType<SyncIntent>(dryRun.Data.Plan.Intent);
        Assert.Equal(fixture.CodexHome, normalizedIntent.CodexHome);
        Assert.Equal("openai", normalizedIntent.ProviderId);
        Assert.False(Directory.Exists(fixture.BackupRoot));
        Assert.Contains("\"model_provider\":\"relay\"", await File.ReadAllTextAsync(fixture.RolloutPath));

        ApplicationOperationPlan plan = dryRun.Data.Plan;
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> applied = await service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.Succeeded, applied.Lifecycle);
        Assert.True(applied.Data!.Applied);
        Assert.Equal("openai", applied.Data.Result!.TargetProvider);
        Assert.True(Directory.Exists(applied.Data.Result.BackupDir));
        Assert.Contains("\"model_provider\":\"openai\"", await File.ReadAllTextAsync(fixture.RolloutPath));
    }

    [Fact]
    public async Task ProductionSync_DriftIsRejectedBeforeCoreCreatesABackup()
    {
        using Fixture fixture = await Fixture.CreateAsync("relay");
        ApplicationService service = CreateService();
        SyncIntent intent = new(fixture.CodexHome, null, "openai", 5);
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> dryRun = await service.SyncAsync(
            new SyncApplicationRequest(intent));
        ApplicationOperationPlan plan = dryRun.Data!.Plan;
        await File.AppendAllTextAsync(
            fixture.RolloutPath,
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\"}}\n");
        string drifted = await File.ReadAllTextAsync(fixture.RolloutPath);

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> applied = await service.SyncAsync(
            new SyncApplicationRequest(
                Assert.IsType<SyncIntent>(plan.Intent),
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.Rejected, applied.Lifecycle);
        Assert.Equal("plan_stale", Assert.Single(applied.Errors).Code);
        Assert.Equal(drifted, await File.ReadAllTextAsync(fixture.RolloutPath));
        Assert.False(Directory.Exists(fixture.BackupRoot));
    }

    private static ApplicationService CreateService()
    {
        return new ApplicationService(
            new CoreApplicationStatusPort(),
            new CoreApplicationWritePort(),
            new InMemoryApplicationPlanLedger());
    }

    private sealed class Fixture : IDisposable
    {
        private Fixture(string root, string codexHome, string rolloutPath)
        {
            Root = root;
            CodexHome = codexHome;
            RolloutPath = rolloutPath;
        }

        public string Root { get; }
        public string CodexHome { get; }
        public string RolloutPath { get; }
        public string BackupRoot => AppConstants.DefaultBackupRoot(CodexHome);

        public static async Task<Fixture> CreateAsync(string provider)
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                $"codex-provider-application-write-{Guid.NewGuid():N}");
            string codexHome = Path.Combine(root, ".codex");
            string sessionDirectory = Path.Combine(codexHome, "sessions", "2026", "08", "04");
            Directory.CreateDirectory(sessionDirectory);
            Directory.CreateDirectory(Path.Combine(codexHome, "archived_sessions"));
            await File.WriteAllTextAsync(
                Path.Combine(codexHome, "config.toml"),
                "model_provider = \"openai\"\n");
            string rolloutPath = Path.Combine(sessionDirectory, "rollout-fixture.jsonl");
            string sessionMeta = JsonSerializer.Serialize(new
            {
                timestamp = "2026-08-04T00:00:00.000Z",
                type = "session_meta",
                payload = new
                {
                    id = "thread-application-write",
                    timestamp = "2026-08-04T00:00:00.000Z",
                    cwd = root,
                    source = "cli",
                    cli_version = "0.115.0",
                    model_provider = provider
                }
            });
            await File.WriteAllTextAsync(rolloutPath, sessionMeta + "\n");
            return new Fixture(root, codexHome, rolloutPath);
        }

        public void Dispose()
        {
            if (!Directory.Exists(Root))
            {
                return;
            }
            string fullRoot = Path.GetFullPath(Root);
            string fullTemp = Path.GetFullPath(Path.GetTempPath());
            string relative = Path.GetRelativePath(fullTemp, fullRoot);
            if (Path.IsPathRooted(relative)
                || relative == ".."
                || relative.StartsWith($"..{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Refusing to remove a test directory outside the temp root.");
            }
            Directory.Delete(fullRoot, recursive: true);
        }
    }
}
