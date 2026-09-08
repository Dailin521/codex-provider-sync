using System.Text.Json;
using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.Automation.Tests;

public sealed class AutomationHostTests
{
    [Fact]
    public async Task DescribeAndStatus_UseTheSharedApplicationServiceContract()
    {
        using TemporaryDirectory temporary = new();
        TestFactory factory = new();
        AutomationHost host = new(factory);

        AutomationRunResult describe = await host.RunAsync(["describe"]);
        AutomationRunResult status = await host.RunAsync(
            ["status", "--codex-home", temporary.Path]);

        Assert.Equal(AutomationExitCodes.Success, describe.ExitCode);
        Assert.Equal("success", describe.Response.Result);
        Assert.Equal(ApplicationProtocol.Version, describe.Response.ProtocolVersion);
        Assert.IsType<ApplicationDescription>(describe.Response.Data);
        Assert.Equal(AutomationExitCodes.Success, status.ExitCode);
        StatusSnapshot snapshot = Assert.IsType<StatusSnapshot>(status.Response.Data);
        Assert.Equal(Path.GetFullPath(temporary.Path), snapshot.CodexHome);
        Assert.Equal(0, factory.Write.ExecuteCalls);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("switch")]
    [InlineData("restore")]
    [InlineData("prune")]
    public async Task EveryWrite_DefaultsToDurablePlanOnly(string command)
    {
        using TemporaryDirectory temporary = new();
        string backup = Directory.CreateDirectory(Path.Combine(temporary.Path, "backup")).FullName;
        string ledger = Path.Combine(temporary.Path, "ledger");
        TestFactory factory = new();
        AutomationHost host = new(factory);
        string[] args = WriteArgs(command, temporary.Path, ledger, backup);

        AutomationRunResult result = await host.RunAsync(args);

        Assert.Equal(AutomationExitCodes.Success, result.ExitCode);
        Assert.Equal("readyToApply", result.Response.Lifecycle);
        Assert.Equal(0, factory.Write.ExecuteCalls);
        Assert.IsAssignableFrom<object>(result.Response.Data);
        Assert.NotEmpty(Directory.EnumerateFiles(Path.Combine(ledger, "entries")));
    }

    [Fact]
    public async Task PlanThenApply_RequiresExactFreshSingleUseDocumentAcrossHostInstances()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string planPath = Path.Combine(temporary.Path, "plan.json");
        TestFactory factory = new();
        AutomationHost planner = new(factory);
        AutomationRunResult planned = await planner.RunAsync(
        [
            "plan", "--operation", "sync", "--codex-home", temporary.Path,
            "--provider", "relay", "--ledger-root", ledger
        ]);
        ApplicationOperationPlan plan = Assert.IsType<ApplicationOperationPlan>(planned.Response.Data);
        await File.WriteAllTextAsync(
            planPath,
            JsonSerializer.Serialize(plan, AutomationJson.Options));
        string[] applyArgs =
        [
            "sync", "--codex-home", temporary.Path, "--provider", "relay",
            "--ledger-root", ledger, "--apply", "--plan", planPath,
            "--plan-digest", plan.Digest
        ];

        AutomationRunResult applied = await new AutomationHost(factory).RunAsync(applyArgs);
        AutomationRunResult duplicate = await new AutomationHost(factory).RunAsync(applyArgs);

        Assert.Equal(AutomationExitCodes.Success, applied.ExitCode);
        Assert.Equal("succeeded", applied.Response.Lifecycle);
        Assert.True(Assert.IsType<ApplicationWriteResult<SyncResult>>(applied.Response.Data).Applied);
        Assert.Equal(AutomationExitCodes.InvalidPlan, duplicate.ExitCode);
        Assert.Equal("plan_already_used", Assert.Single(duplicate.Response.Errors).Code);
        Assert.Equal(1, factory.Write.ExecuteCalls);
    }

    [Fact]
    public async Task TamperedMalformedAndExpiredPlans_AreRejectedBeforeCoreExecution()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string planPath = Path.Combine(temporary.Path, "plan.json");
        TestClock clock = new(DateTimeOffset.Parse("2026-08-04T00:00:00Z"));
        TestFactory factory = new(clock);
        AutomationHost host = new(factory);
        AutomationRunResult planned = await host.RunAsync(
        [
            "sync", "--codex-home", temporary.Path, "--provider", "relay",
            "--ledger-root", ledger
        ]);
        ApplicationWriteResult<SyncResult> dryRun = Assert.IsType<ApplicationWriteResult<SyncResult>>(planned.Response.Data);
        ApplicationOperationPlan plan = dryRun.Plan;
        ApplicationOperationPlan tampered = plan with
        {
            Targets = [new ApplicationPlanTarget(Path.Combine(temporary.Path, "other"), "replace", "changed")]
        };
        await File.WriteAllTextAsync(planPath, JsonSerializer.Serialize(tampered, AutomationJson.Options));
        string[] apply =
        [
            "sync", "--codex-home", temporary.Path, "--provider", "relay",
            "--ledger-root", ledger, "--apply", "--plan", planPath,
            "--plan-digest", plan.Digest
        ];

        AutomationRunResult changed = await host.RunAsync(apply);
        await File.WriteAllTextAsync(planPath, "{\"unknown\":true}");
        AutomationRunResult malformed = await host.RunAsync(apply);
        await File.WriteAllTextAsync(planPath, JsonSerializer.Serialize(plan, AutomationJson.Options));
        clock.Advance(TimeSpan.FromMinutes(11));
        AutomationRunResult expired = await host.RunAsync(apply);

        Assert.Equal("plan_digest_mismatch", Assert.Single(changed.Response.Errors).Code);
        Assert.Equal("plan_document_malformed", Assert.Single(malformed.Response.Errors).Code);
        Assert.Equal("plan_expired", Assert.Single(expired.Response.Errors).Code);
        Assert.All(
            new[] { changed.ExitCode, malformed.ExitCode, expired.ExitCode },
            code => Assert.Equal(AutomationExitCodes.InvalidPlan, code));
        Assert.Equal(0, factory.Write.ExecuteCalls);
    }

    [Fact]
    public async Task TimeoutAndConcurrentStatus_HaveStableExitCodes()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        TestFactory timeoutFactory = new();
        timeoutFactory.Write.PlanHandler = async (_, _, token) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            throw new InvalidOperationException("unreachable");
        };
        AutomationRunResult timedOut = await new AutomationHost(timeoutFactory).RunAsync(
        [
            "plan", "--operation", "sync", "--codex-home", temporary.Path,
            "--provider", "relay", "--ledger-root", ledger, "--timeout-ms", "20"
        ]);

        TestFactory busyFactory = new();
        TaskCompletionSource<ApplicationPlanPreview> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        busyFactory.Write.PlanHandler = (_, _, _) =>
        {
            started.TrySetResult();
            return pending.Task;
        };
        IApplicationService shared = busyFactory.CreateService(
            new FileApplicationPlanLedger(Path.Combine(temporary.Path, "busy-ledger")));
        AutomationHost busyHost = new(new FixedServiceFactory(shared));
        Task<AutomationRunResult> active = busyHost.RunAsync(
        [
            "plan", "--operation", "sync", "--codex-home", temporary.Path,
            "--provider", "relay", "--ledger-root", Path.Combine(temporary.Path, "busy-ledger")
        ]);
        await started.Task;
        AutomationRunResult status = await busyHost.RunAsync(
            ["status", "--codex-home", temporary.Path]);
        pending.SetResult(busyFactory.Write.Preview(new SyncIntent(temporary.Path, null, "relay")));
        await active;

        Assert.Equal(AutomationExitCodes.CancelledOrTimedOut, timedOut.ExitCode);
        Assert.Equal("timeout", Assert.Single(timedOut.Response.Errors).Code);
        Assert.Equal(AutomationExitCodes.Success, status.ExitCode);
        Assert.Equal("succeeded", status.Response.Lifecycle);
        Assert.Empty(status.Response.Errors);
    }

    [Fact]
    public async Task CallerCancellation_IsDistinctFromTimeout()
    {
        using TemporaryDirectory temporary = new();
        TestFactory factory = new();
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        factory.Write.PlanHandler = async (_, _, token) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            throw new InvalidOperationException("unreachable");
        };
        using CancellationTokenSource cancellation = new();
        Task<AutomationRunResult> running = new AutomationHost(factory).RunAsync(
        [
            "plan", "--operation", "sync", "--codex-home", temporary.Path,
            "--provider", "relay", "--ledger-root", Path.Combine(temporary.Path, "ledger")
        ], cancellation.Token);
        await started.Task;

        cancellation.Cancel();
        AutomationRunResult result = await running;

        Assert.Equal(AutomationExitCodes.CancelledOrTimedOut, result.ExitCode);
        Assert.Equal("cancelled", result.Response.Lifecycle);
        Assert.Equal("cancelled", Assert.Single(result.Response.Errors).Code);
    }

    [Fact]
    public async Task RollbackAndRecoveryOutcomes_AreDistinct()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string planPath = Path.Combine(temporary.Path, "plan.json");
        TestFactory factory = new();
        ApplicationOperationPlan rollbackPlan = await CreateSyncPlanAsync(factory, temporary.Path, ledger, planPath);
        factory.Write.SyncHandler = (_, _, _, _) => throw new SyncTransactionException(
            new IOException("injected"),
            [],
            Path.Combine(temporary.Path, "backup"),
            [],
            [],
            rollbackStatus: "complete",
            recoveryRequired: false);
        AutomationRunResult rollback = await ApplySyncAsync(factory, temporary.Path, ledger, planPath, rollbackPlan.Digest);

        string recoveryLedger = Path.Combine(temporary.Path, "recovery-ledger");
        ApplicationOperationPlan recoveryPlan = await CreateSyncPlanAsync(factory, temporary.Path, recoveryLedger, planPath);
        factory.Write.SyncHandler = (_, _, _, _) => throw new ApplicationPortException(
            "recovery_required",
            "rollback incomplete",
            recoveryRequired: true,
            rollbackStatus: "incomplete");
        AutomationRunResult recovery = await ApplySyncAsync(factory, temporary.Path, recoveryLedger, planPath, recoveryPlan.Digest);

        Assert.Equal(AutomationExitCodes.RolledBackFailure, rollback.ExitCode);
        Assert.Equal("rollback", rollback.Response.Result);
        Assert.Equal(AutomationExitCodes.RecoveryRequired, recovery.ExitCode);
        Assert.Equal("recovery", recovery.Response.Result);
    }

    [Fact]
    public async Task FailedBeforeApplying_UsesValidationExitCode()
    {
        using TemporaryDirectory temporary = new();
        TestFactory factory = new();
        factory.Write.PlanHandler = (_, _, _) => throw new ApplicationPortException(
            "provider_missing",
            "The selected provider is not configured.");

        AutomationRunResult result = await new AutomationHost(factory).RunAsync(
        [
            "switch", "--codex-home", temporary.Path, "--provider", "missing",
            "--ledger-root", Path.Combine(temporary.Path, "ledger")
        ]);

        Assert.Equal(AutomationExitCodes.ValidationOrUsage, result.ExitCode);
        Assert.Equal("failed", result.Response.Lifecycle);
        Assert.DoesNotContain(result.Response.Timeline, static entry =>
            entry.Lifecycle == ApplicationOperationLifecycle.Applying);
        Assert.Equal(0, factory.Write.ExecuteCalls);
    }

    [Fact]
    public async Task FailedAfterApplyingWithoutCompleteRollback_FailsClosed()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string planPath = Path.Combine(temporary.Path, "plan.json");
        TestFactory factory = new();
        ApplicationOperationPlan plan = await CreateSyncPlanAsync(factory, temporary.Path, ledger, planPath);
        factory.Write.SyncHandler = (_, _, _, _) => throw new ApplicationPortException(
            "operation_failed",
            "The mutation outcome is unknown.");

        AutomationRunResult result = await ApplySyncAsync(
            factory,
            temporary.Path,
            ledger,
            planPath,
            plan.Digest);

        Assert.Equal(AutomationExitCodes.InternalProtocolFailure, result.ExitCode);
        Assert.Contains(result.Response.Timeline, static entry =>
            entry.Lifecycle == ApplicationOperationLifecycle.Applying);
    }

    private static async Task<ApplicationOperationPlan> CreateSyncPlanAsync(
        TestFactory factory,
        string home,
        string ledger,
        string planPath)
    {
        AutomationRunResult result = await new AutomationHost(factory).RunAsync(
        [
            "sync", "--codex-home", home, "--provider", "relay", "--ledger-root", ledger
        ]);
        ApplicationOperationPlan plan = Assert.IsType<ApplicationWriteResult<SyncResult>>(result.Response.Data).Plan;
        await File.WriteAllTextAsync(planPath, JsonSerializer.Serialize(plan, AutomationJson.Options));
        return plan;
    }

    private static Task<AutomationRunResult> ApplySyncAsync(
        TestFactory factory,
        string home,
        string ledger,
        string planPath,
        string digest)
    {
        return new AutomationHost(factory).RunAsync(
        [
            "sync", "--codex-home", home, "--provider", "relay", "--ledger-root", ledger,
            "--apply", "--plan", planPath, "--plan-digest", digest
        ]);
    }

    private static string[] WriteArgs(string command, string home, string ledger, string backup)
    {
        List<string> args = [command, "--codex-home", home, "--ledger-root", ledger];
        switch (command)
        {
            case "sync":
                args.AddRange(["--provider", "relay"]);
                break;
            case "switch":
                args.AddRange(["--provider", "relay", "--model-mode", "keep-root"]);
                break;
            case "restore":
                args.AddRange(["--backup", backup]);
                break;
            case "prune":
                args.AddRange(["--keep", "3"]);
                break;
        }
        return args.ToArray();
    }
}

internal sealed class FixedServiceFactory(IApplicationService service) : IAutomationApplicationFactory
{
    public IApplicationService Create(AutomationInvocation invocation) => service;
}

internal sealed class TestFactory : IAutomationApplicationFactory
{
    private int _nextId;

    public TestFactory(TestClock? clock = null)
    {
        Clock = clock ?? new TestClock(DateTimeOffset.Parse("2026-08-04T00:00:00Z"));
    }

    public TestClock Clock { get; }

    public TestWritePort Write { get; } = new();

    public IApplicationService Create(AutomationInvocation invocation)
    {
        IApplicationPlanLedger ledger = invocation.IsWrite
            ? new FileApplicationPlanLedger(invocation.LedgerRoot!)
            : new InMemoryApplicationPlanLedger();
        return CreateService(ledger);
    }

    public IApplicationService CreateService(IApplicationPlanLedger ledger)
    {
        return new ApplicationService(
            new TestStatusPort(),
            Write,
            ledger,
            Clock,
            () => $"test-{Interlocked.Increment(ref _nextId):D6}");
    }
}

internal sealed class TestStatusPort : IApplicationStatusPort
{
    public Task<StatusSnapshot> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(TestData.Status(Path.GetFullPath(request.CodexHome)));
    }
}

internal sealed class TestWritePort : IApplicationWritePort
{
    public int ExecuteCalls;

    public Func<ApplicationWriteIntent, string, CancellationToken, Task<ApplicationPlanPreview>>? PlanHandler { get; set; }

    public Func<SyncIntent, ApplicationOperationPlan, string, CancellationToken, Task<SyncResult>>? SyncHandler { get; set; }

    public Task<ApplicationPlanPreview> CreatePlanAsync(
        ApplicationWriteIntent intent,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        return PlanHandler?.Invoke(intent, operationId, cancellationToken)
            ?? Task.FromResult(Preview(intent));
    }

    public ApplicationPlanPreview Preview(ApplicationWriteIntent intent)
    {
        return new ApplicationPlanPreview(
            intent,
            "state-fixture",
            "execution-fixture",
            [new ApplicationPlanTarget(Path.Combine(intent.CodexHome, "config.toml"), "replace", "sha256:fixture")]);
    }

    public async Task<SyncResult> ExecuteSyncAsync(
        SyncIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref ExecuteCalls);
        return SyncHandler is null
            ? TestData.Sync(intent)
            : await SyncHandler(intent, plan, operationId, cancellationToken);
    }

    public Task<SyncResult> ExecuteSwitchAsync(
        SwitchIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref ExecuteCalls);
        return Task.FromResult(TestData.Sync(intent));
    }

    public Task<RestoreResult> ExecuteRestoreAsync(
        RestoreIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref ExecuteCalls);
        return Task.FromResult(new RestoreResult
        {
            CodexHome = intent.CodexHome,
            BackupDir = intent.BackupDirectory,
            TargetProvider = "relay"
        });
    }

    public Task<BackupPruneResult> ExecutePruneAsync(
        PruneIntent intent,
        ApplicationOperationPlan plan,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref ExecuteCalls);
        return Task.FromResult(new BackupPruneResult
        {
            BackupRoot = Path.Combine(intent.CodexHome, "backups_state", "provider-sync"),
            DeletedCount = 1,
            RemainingCount = intent.BackupRetentionCount,
            FreedBytes = 10
        });
    }
}

internal sealed class TestClock(DateTimeOffset now) : TimeProvider
{
    private DateTimeOffset _now = now;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan value) => _now = _now.Add(value);
}

internal static class TestData
{
    public static StatusSnapshot Status(string home) => new()
    {
        CodexHome = home,
        SqliteHome = Path.Combine(home, "sqlite"),
        CurrentProvider = new CurrentProviderInfo("openai", false),
        ConfiguredProviders = ["openai", "relay"],
        RolloutCounts = new ProviderCounts(),
        LockedRolloutFiles = [],
        UnreadableRolloutFiles = [],
        EncryptedContentCounts = new ProviderCounts(),
        SqliteCounts = null,
        BackupRoot = Path.Combine(home, "backups_state", "provider-sync"),
        BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
    };

    public static SyncResult Sync(ApplicationWriteIntent intent) => new()
    {
        CodexHome = intent.CodexHome,
        TargetProvider = intent switch
        {
            SyncIntent sync => sync.ProviderId,
            SwitchIntent change => change.ProviderId,
            _ => "relay"
        },
        PreviousProvider = "openai",
        BackupDir = Path.Combine(intent.CodexHome, "backups_state", "provider-sync", "fixture"),
        ChangedSessionFiles = 0,
        SkippedLockedRolloutFiles = [],
        SkippedUnreadableRolloutFiles = [],
        SqliteRowsUpdated = 0,
        SqlitePresent = false,
        RolloutCountsBefore = new ProviderCounts(),
        EncryptedContentCounts = new ProviderCounts()
    };
}
