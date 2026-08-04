using System.Text.Json;
using CodexProviderSync.Core;

namespace CodexProviderSync.Application.Tests;

public sealed class ApplicationServiceTests
{
    [Fact]
    public async Task Describe_ReportsTheSharedDryRunContractWithStructuredLifecycle()
    {
        TestRig rig = new();

        ApplicationOutcome<ApplicationDescription> outcome = await rig.Service.DescribeAsync();

        Assert.Equal(ApplicationOperationLifecycle.Succeeded, outcome.Lifecycle);
        Assert.Equal(ApplicationOperationKind.Describe, outcome.Operation);
        Assert.Equal(ApplicationProtocol.Version, outcome.Data!.ProtocolVersion);
        Assert.Equal(
            ["describe", "status", "plan", "sync", "switch", "restore", "prune"],
            outcome.Data.Commands);
        Assert.True(outcome.Data.WritesDefaultToDryRun);
        Assert.True(outcome.Data.ExplicitApplyRequired);
        Assert.True(outcome.Data.ExactPlanDigestRequired);
        Assert.True(outcome.Data.PlansAreSingleUse);
        Assert.NotEmpty(outcome.OperationId);
        Assert.Equal(ApplicationOperationLifecycle.Accepted, outcome.Timeline[0].Lifecycle);
        Assert.Equal(ApplicationOperationLifecycle.Succeeded, outcome.Timeline[^1].Lifecycle);
        Assert.Empty(outcome.Errors);
    }

    [Fact]
    public async Task Status_UsesThePureReadPortAndNeverTouchesTheWritePort()
    {
        TestRig rig = new();

        ApplicationOutcome<StatusSnapshot> outcome = await rig.Service.GetStatusAsync(
            new ApplicationStatusRequest(" /fixture ", " /sqlite "));

        Assert.Equal(ApplicationOperationLifecycle.Succeeded, outcome.Lifecycle);
        ApplicationStatusRequest request = Assert.Single(rig.Status.Requests);
        Assert.Equal(" /fixture ", request.CodexHome);
        Assert.Equal(" /sqlite ", request.SqliteHomeOverride);
        Assert.Equal(0, rig.Write.PlanCalls);
        Assert.Equal(0, rig.Write.ExecuteCalls);
    }

    [Fact]
    public async Task CoreStatusPort_DoesNotPersistSettingsOrMutateTheFixtureHome()
    {
        string fixture = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-sync-application-status-{Guid.NewGuid():N}");
        Directory.CreateDirectory(fixture);
        try
        {
            string configPath = Path.Combine(fixture, "config.toml");
            await File.WriteAllTextAsync(configPath, "model_provider = \"openai\"\n");
            Dictionary<string, (long Length, long LastWrite)> before = SnapshotFiles(fixture);
            CoreApplicationStatusPort port = new();

            StatusSnapshot status = await port.GetStatusAsync(new ApplicationStatusRequest(fixture));

            Assert.Equal(Path.GetFullPath(fixture), status.CodexHome);
            Assert.Equal(before, SnapshotFiles(fixture));
        }
        finally
        {
            Directory.Delete(fixture, recursive: true);
        }
    }

    [Fact]
    public async Task EveryWrite_DefaultsToPlanOnlyAndNeverExecutes()
    {
        TestRig rig = new();

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> sync = await rig.Service.SyncAsync(
            new SyncApplicationRequest(new SyncIntent("/fixture", null, "relay")));
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> change = await rig.Service.SwitchAsync(
            new SwitchApplicationRequest(new SwitchIntent(
                "/fixture",
                null,
                "relay",
                new KeepRootModelSelection())));
        ApplicationOutcome<ApplicationWriteResult<RestoreResult>> restore = await rig.Service.RestoreAsync(
            new RestoreApplicationRequest(new RestoreIntent(
                "/fixture",
                "/sqlite",
                "/fixture/backups/one",
                AllowSqliteHomeRelocation: true)));
        ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>> prune = await rig.Service.PruneAsync(
            new PruneApplicationRequest(new PruneIntent("/fixture", null, 3)));

        Assert.All(
            new[] { sync.Lifecycle, change.Lifecycle, restore.Lifecycle, prune.Lifecycle },
            lifecycle => Assert.Equal(ApplicationOperationLifecycle.ReadyToApply, lifecycle));
        Assert.False(sync.Data!.Applied);
        Assert.False(change.Data!.Applied);
        Assert.False(restore.Data!.Applied);
        Assert.False(prune.Data!.Applied);
        Assert.Null(sync.Data.Result);
        Assert.Null(change.Data.Result);
        Assert.Null(restore.Data.Result);
        Assert.Null(prune.Data.Result);
        Assert.Equal(4, rig.Write.PlanCalls);
        Assert.Equal(0, rig.Write.ExecuteCalls);
        RestoreIntent plannedRestore = Assert.IsType<RestoreIntent>(restore.Data.Plan.Intent);
        Assert.True(plannedRestore.AllowSqliteHomeRelocation);
    }

    [Fact]
    public async Task Plan_ContainsDeterministicTargetsWarningsAndAutoPruneDeletionSet()
    {
        TestRig rig = new();
        List<ApplicationPlanTarget> targets =
        [
            new("/fixture/z", "replace", "z"),
            new("/fixture/a", "replace", "a")
        ];
        List<ApplicationPlanTarget> deletions =
        [
            new("/fixture/backups/old-2", "delete", "d2"),
            new("/fixture/backups/old-1", "delete", "d1")
        ];
        List<ApplicationWarning> warnings = [new("locked", "one active rollout is locked")];
        rig.Write.PreviewFactory = intent => new ApplicationPlanPreview(
            intent,
            "state-v1",
            "core-token",
            targets,
            deletions,
            warnings);

        ApplicationOutcome<ApplicationOperationPlan> outcome = await rig.Service.CreatePlanAsync(
            new CreateApplicationPlanRequest(new SyncIntent("/fixture", null, "relay")));
        targets.Clear();
        deletions.Clear();
        warnings.Clear();

        ApplicationOperationPlan plan = outcome.Data!;
        Assert.Equal(["/fixture/a", "/fixture/z"], plan.Targets.Select(static target => target.Path));
        Assert.Equal(
            ["/fixture/backups/old-1", "/fixture/backups/old-2"],
            plan.AutoPruneDeletionTargets.Select(static target => target.Path));
        Assert.Single(plan.Warnings);
        Assert.Equal(64, plan.Digest.Length);
        Assert.Equal(ApplicationOperationLifecycle.ReadyToApply, outcome.Lifecycle);
    }

    [Fact]
    public async Task Apply_RequiresTheExactRegisteredPlanAndConsumesItOnce()
    {
        TestRig rig = new();
        SyncIntent intent = new("/fixture", null, "relay");
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> dryRun = await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent));
        ApplicationOperationPlan plan = dryRun.Data!.Plan;
        SyncApplicationRequest apply = new(
            intent,
            new ApplicationApplyAuthorization(true, plan, plan.Digest));

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> applied = await rig.Service.SyncAsync(apply);
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> duplicate = await rig.Service.SyncAsync(apply);

        Assert.Equal(ApplicationOperationLifecycle.Succeeded, applied.Lifecycle);
        Assert.True(applied.Data!.Applied);
        Assert.Same(rig.Write.SyncResult, applied.Data.Result);
        Assert.Equal(1, rig.Write.ExecuteCalls);
        Assert.Equal(plan.PlanId, rig.Write.LastExecutedPlan!.PlanId);
        Assert.Equal(ApplicationOperationLifecycle.Rejected, duplicate.Lifecycle);
        Assert.Equal("plan_already_used", Assert.Single(duplicate.Errors).Code);
        Assert.Equal(1, rig.Write.ExecuteCalls);
    }

    [Fact]
    public async Task Apply_RejectsMissingMismatchedTamperedAndExpiredPlansWithoutExecuting()
    {
        TestRig rig = new();
        SyncIntent intent = new("/fixture", null, "relay");
        ApplicationOperationPlan plan = (await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> missing = await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent, new ApplicationApplyAuthorization(Apply: true)));
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> mismatch = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent with { ProviderId = "other" },
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));
        ApplicationOperationPlan tampered = plan with
        {
            Targets = [new ApplicationPlanTarget("/fixture/other", "replace", "changed")]
        };
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> changed = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, tampered, tampered.Digest)));
        rig.Clock.Advance(TimeSpan.FromMinutes(11));
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> expired = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal("plan_required", Assert.Single(missing.Errors).Code);
        Assert.Equal("plan_input_mismatch", Assert.Single(mismatch.Errors).Code);
        Assert.Equal("plan_digest_mismatch", Assert.Single(changed.Errors).Code);
        Assert.Equal("plan_expired", Assert.Single(expired.Errors).Code);
        Assert.Equal(0, rig.Write.ExecuteCalls);
    }

    [Fact]
    public async Task ConcurrentOperation_IsRejectedImmediatelyWithoutReplacingTheActivePlan()
    {
        TestRig rig = new();
        TaskCompletionSource<ApplicationPlanPreview> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Write.PreviewHandler = (intent, _, _) =>
        {
            started.SetResult();
            return pending.Task;
        };

        Task<ApplicationOutcome<ApplicationOperationPlan>> active = rig.Service.CreatePlanAsync(
            new CreateApplicationPlanRequest(new SyncIntent("/first", null, "relay")));
        await started.Task;
        ApplicationOutcome<StatusSnapshot> rejected = await rig.Service.GetStatusAsync(
            new ApplicationStatusRequest("/second"));

        Assert.Equal(ApplicationOperationLifecycle.Rejected, rejected.Lifecycle);
        Assert.Equal("operation_busy", Assert.Single(rejected.Errors).Code);
        Assert.Empty(rig.Status.Requests);

        pending.SetResult(rig.Write.CreatePreview(new SyncIntent("/first", null, "relay")));
        ApplicationOutcome<ApplicationOperationPlan> completed = await active;
        Assert.Equal("/first", completed.Data!.Intent.CodexHome);
    }

    [Fact]
    public async Task Cancellation_ConsumesThePlanReturnsCancelledAndReleasesTheGate()
    {
        TestRig rig = new();
        SyncIntent intent = new("/fixture", null, "relay");
        ApplicationOperationPlan plan = (await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;
        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Write.SyncHandler = async (_, _, _, token) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            return rig.Write.SyncResult;
        };
        using CancellationTokenSource cancellation = new();

        Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> applying = rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)),
            cancellation.Token);
        await started.Task;
        cancellation.Cancel();
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> cancelled = await applying;
        ApplicationOutcome<ApplicationDescription> next = await rig.Service.DescribeAsync();
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> duplicate = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.Cancelled, cancelled.Lifecycle);
        Assert.Equal("cancelled", Assert.Single(cancelled.Errors).Code);
        Assert.Equal(ApplicationOperationLifecycle.Succeeded, next.Lifecycle);
        Assert.Equal("plan_already_used", Assert.Single(duplicate.Errors).Code);
    }

    [Fact]
    public async Task RolledBackCancellationAndRecoveryFailureHaveDistinctStructuredOutcomes()
    {
        TestRig cancelRig = new();
        SyncIntent intent = new("/fixture", null, "relay");
        ApplicationOperationPlan cancelPlan = (await cancelRig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;
        cancelRig.Write.SyncHandler = (_, _, _, _) => throw new SyncTransactionException(
            new OperationCanceledException("cancelled after a rollout"),
            [],
            "/fixture/backups/cancel",
            ["/fixture/rollout.jsonl"],
            [],
            rollbackStatus: "complete",
            recoveryRequired: false);

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> cancelled = await cancelRig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, cancelPlan, cancelPlan.Digest)));

        TestRig recoveryRig = new();
        ApplicationOperationPlan recoveryPlan = (await recoveryRig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;
        recoveryRig.Write.SyncHandler = (_, _, _, _) => throw new ApplicationPortException(
            "recovery_required",
            "rollback could not restore SQLite",
            recoveryRequired: true,
            rollbackStatus: "incomplete");
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> recovery = await recoveryRig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, recoveryPlan, recoveryPlan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.Cancelled, cancelled.Lifecycle);
        Assert.Equal("complete", Assert.Single(cancelled.Errors).RollbackStatus);
        Assert.Equal(ApplicationOperationLifecycle.RecoveryRequired, recovery.Lifecycle);
        ApplicationError recoveryError = Assert.Single(recovery.Errors);
        Assert.True(recoveryError.RecoveryRequired);
        Assert.Equal("incomplete", recoveryError.RollbackStatus);
    }

    [Fact]
    public async Task PolymorphicPlan_RoundTripsAcrossAOneShotProcessBoundary()
    {
        TestRig rig = new();
        SwitchIntent intent = new(
            "/fixture",
            "/sqlite",
            "relay",
            new CustomModelSelection("model-x"),
            4);
        ApplicationOperationPlan plan = (await rig.Service.SwitchAsync(
            new SwitchApplicationRequest(intent))).Data!.Plan;
        JsonSerializerOptions options = new(JsonSerializerDefaults.Web);

        string json = JsonSerializer.Serialize(plan, options);
        ApplicationOperationPlan restored = JsonSerializer.Deserialize<ApplicationOperationPlan>(json, options)!;
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> applied = await rig.Service.SwitchAsync(
            new SwitchApplicationRequest(
                Assert.IsType<SwitchIntent>(restored.Intent),
                new ApplicationApplyAuthorization(true, restored, restored.Digest)));

        Assert.Contains("\"kind\":\"switch\"", json, StringComparison.Ordinal);
        Assert.Contains("\"mode\":\"custom\"", json, StringComparison.Ordinal);
        Assert.Equal(ApplicationOperationLifecycle.Succeeded, applied.Lifecycle);
        SwitchIntent executed = Assert.IsType<SwitchIntent>(rig.Write.LastIntent);
        Assert.Equal("model-x", Assert.IsType<CustomModelSelection>(executed.ModelSelection).Model);
    }

    [Fact]
    public async Task EveryIntentKind_HasAStableExplicitJsonDiscriminator()
    {
        TestRig rig = new();
        JsonSerializerOptions options = new(JsonSerializerDefaults.Web);
        ApplicationOperationPlan[] plans =
        [
            (await rig.Service.SyncAsync(new SyncApplicationRequest(
                new SyncIntent("/fixture", null, "relay")))).Data!.Plan,
            (await rig.Service.SwitchAsync(new SwitchApplicationRequest(
                new SwitchIntent("/fixture", null, "relay", new FollowProviderModelSelection())))).Data!.Plan,
            (await rig.Service.RestoreAsync(new RestoreApplicationRequest(
                new RestoreIntent("/fixture", "/sqlite", "/fixture/backups/one", AllowSqliteHomeRelocation: true)))).Data!.Plan,
            (await rig.Service.PruneAsync(new PruneApplicationRequest(
                new PruneIntent("/fixture", null, 2)))).Data!.Plan
        ];

        ApplicationWriteIntent[] restored = plans
            .Select(plan => JsonSerializer.Deserialize<ApplicationOperationPlan>(
                JsonSerializer.Serialize(plan, options),
                options)!.Intent)
            .ToArray();

        Assert.IsType<SyncIntent>(restored[0]);
        Assert.IsType<SwitchIntent>(restored[1]);
        Assert.True(Assert.IsType<RestoreIntent>(restored[2]).AllowSqliteHomeRelocation);
        Assert.IsType<PruneIntent>(restored[3]);
    }

    [Fact]
    public async Task ValidPlan_FromAnotherLedgerIsRejectedBeforeCoreExecution()
    {
        TestRig planner = new();
        SyncIntent intent = new("/fixture", null, "relay");
        ApplicationOperationPlan plan = (await planner.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;
        TestRig executorWithDifferentLedger = new();

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> outcome =
            await executorWithDifferentLedger.Service.SyncAsync(
                new SyncApplicationRequest(
                    intent,
                    new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.Rejected, outcome.Lifecycle);
        Assert.Equal("plan_not_registered", Assert.Single(outcome.Errors).Code);
        Assert.Equal(0, executorWithDifferentLedger.Write.ExecuteCalls);
    }

    [Fact]
    public async Task CorruptDurableLedger_FailsClosedWithControlledStructuredEvidence()
    {
        using TemporaryApplicationDirectory temporary = new();
        string ledgerRoot = Path.Combine(temporary.Path, "ledger");
        FileApplicationPlanLedger ledger = new(ledgerRoot);
        TestRig rig = new(ledger);
        SyncIntent intent = new("/isolated/fixture", null, "relay");
        ApplicationOperationPlan plan = (await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;
        string registrationPath = Assert.Single(Directory.EnumerateFiles(
            Path.Combine(ledgerRoot, "entries"),
            "*.registration.v1.json"));
        await File.WriteAllTextAsync(registrationPath, "{\"schemaVersion\":1");

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> outcome = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(ApplicationOperationLifecycle.RecoveryRequired, outcome.Lifecycle);
        Assert.Null(outcome.Data);
        Assert.Equal(0, rig.Write.ExecuteCalls);
        ApplicationError error = Assert.Single(outcome.Errors);
        Assert.Equal("plan_ledger_corrupt", error.Code);
        Assert.True(error.RecoveryRequired);
        Assert.Equal(Path.GetFullPath(registrationPath), error.EvidencePath);
        Assert.DoesNotContain(registrationPath, error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(plan.PlanId, error.Message, StringComparison.Ordinal);
        Assert.Equal(ApplicationOperationLifecycle.RecoveryRequired, outcome.Timeline[^1].Lifecycle);
    }

    [Fact]
    public async Task CorruptCompletionReceipt_IsNotDowngradedToAWarningAfterExecution()
    {
        string evidencePath = Path.GetFullPath(Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-plan-ledger-evidence-{Guid.NewGuid():N}.json"));
        CorruptOnCompleteLedger ledger = new(evidencePath);
        TestRig rig = new(ledger);
        SyncIntent intent = new("/isolated/fixture", null, "relay");
        ApplicationOperationPlan plan = (await rig.Service.SyncAsync(
            new SyncApplicationRequest(intent))).Data!.Plan;

        ApplicationOutcome<ApplicationWriteResult<SyncResult>> outcome = await rig.Service.SyncAsync(
            new SyncApplicationRequest(
                intent,
                new ApplicationApplyAuthorization(true, plan, plan.Digest)));

        Assert.Equal(1, rig.Write.ExecuteCalls);
        Assert.Equal(ApplicationOperationLifecycle.RecoveryRequired, outcome.Lifecycle);
        Assert.Null(outcome.Data);
        Assert.Empty(outcome.Warnings);
        ApplicationError error = Assert.Single(outcome.Errors);
        Assert.Equal("plan_ledger_corrupt", error.Code);
        Assert.True(error.RecoveryRequired);
        Assert.Equal(evidencePath, error.EvidencePath);
        Assert.DoesNotContain("sensitive ledger detail", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(evidencePath, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InMemoryLedger_AtomicallyAllowsOnlyOneConcurrentClaim()
    {
        InMemoryApplicationPlanLedger ledger = new();
        ApplicationOperationPlan plan = new(
            ApplicationProtocol.Version,
            "plan-one",
            "operation-one",
            new DateTimeOffset(2026, 8, 4, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 8, 4, 0, 10, 0, TimeSpan.Zero),
            new SyncIntent("/fixture", null, "relay"),
            "state",
            "token",
            [new ApplicationPlanTarget("/fixture/target", "replace", "fingerprint")],
            [],
            [],
            "digest-one");
        await ledger.RegisterAsync(plan);

        ApplicationPlanClaimResult[] claims = await Task.WhenAll(
            Enumerable.Range(0, 20)
                .Select(_ => ledger.TryClaimAsync(plan.PlanId, plan.Digest)));

        Assert.Single(claims, static claim => claim.Status == ApplicationPlanClaimStatus.Claimed);
        Assert.Equal(19, claims.Count(static claim => claim.Status == ApplicationPlanClaimStatus.AlreadyUsed));
    }

    [Fact]
    public async Task InMemoryLedger_CompletionMatchesDurableSingleUseTerminalSemantics()
    {
        InMemoryApplicationPlanLedger ledger = new();
        ApplicationOperationPlan plan = new(
            ApplicationProtocol.Version,
            "plan-terminal",
            "operation-terminal",
            new DateTimeOffset(2026, 8, 4, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 8, 4, 0, 10, 0, TimeSpan.Zero),
            new SyncIntent("/fixture", null, "relay"),
            "state",
            "token",
            [new ApplicationPlanTarget("/fixture/target", "replace", "fingerprint")],
            [],
            [],
            "digest-terminal");
        await ledger.RegisterAsync(plan);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded));
        Assert.Equal(
            ApplicationPlanClaimStatus.Claimed,
            (await ledger.TryClaimAsync(plan.PlanId, plan.Digest)).Status);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Applying));

        await ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded);
        await ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded);
        InvalidOperationException transition = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Failed));

        Assert.Contains("cannot transition", transition.Message, StringComparison.Ordinal);
        Assert.Equal(
            ApplicationPlanClaimStatus.AlreadyUsed,
            (await ledger.TryClaimAsync(plan.PlanId, plan.Digest)).Status);
    }

    [Fact]
    public async Task SwitchRestoreAndPrune_ExecuteOnlyThroughTheirTypedPortMethods()
    {
        TestRig rig = new();

        SwitchIntent change = new("/fixture", null, "relay", new FollowProviderModelSelection());
        ApplicationOperationPlan changePlan = (await rig.Service.SwitchAsync(
            new SwitchApplicationRequest(change))).Data!.Plan;
        await rig.Service.SwitchAsync(new SwitchApplicationRequest(
            change,
            new ApplicationApplyAuthorization(true, changePlan, changePlan.Digest)));

        RestoreIntent restore = new(
            "/fixture",
            "/sqlite",
            "/fixture/backups/one",
            RestoreConfig: false,
            RestoreDatabase: true,
            RestoreSessions: true,
            AllowSqliteHomeRelocation: true);
        ApplicationOperationPlan restorePlan = (await rig.Service.RestoreAsync(
            new RestoreApplicationRequest(restore))).Data!.Plan;
        await rig.Service.RestoreAsync(new RestoreApplicationRequest(
            restore,
            new ApplicationApplyAuthorization(true, restorePlan, restorePlan.Digest)));

        PruneIntent prune = new("/fixture", null, 2);
        ApplicationOperationPlan prunePlan = (await rig.Service.PruneAsync(
            new PruneApplicationRequest(prune))).Data!.Plan;
        await rig.Service.PruneAsync(new PruneApplicationRequest(
            prune,
            new ApplicationApplyAuthorization(true, prunePlan, prunePlan.Digest)));

        Assert.Equal(1, rig.Write.SwitchExecuteCalls);
        Assert.Equal(1, rig.Write.RestoreExecuteCalls);
        Assert.Equal(1, rig.Write.PruneExecuteCalls);
        Assert.True(Assert.IsType<RestoreIntent>(rig.Write.ExecutedIntents[1]).AllowSqliteHomeRelocation);
    }

    private sealed class TestRig
    {
        private int _nextId;

        public TestRig(IApplicationPlanLedger? planLedger = null)
        {
            Service = new ApplicationService(
                Status,
                Write,
                planLedger ?? new InMemoryApplicationPlanLedger(),
                Clock,
                () => $"id-{Interlocked.Increment(ref _nextId)}",
                TimeSpan.FromMinutes(10));
        }

        public FakeTimeProvider Clock { get; } = new(new DateTimeOffset(2026, 8, 4, 0, 0, 0, TimeSpan.Zero));

        public FakeStatusPort Status { get; } = new();

        public FakeWritePort Write { get; } = new();

        public ApplicationService Service { get; }
    }

    private sealed class FakeTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow = _utcNow.Add(duration);
    }

    private sealed class FakeStatusPort : IApplicationStatusPort
    {
        public List<ApplicationStatusRequest> Requests { get; } = [];

        public Task<StatusSnapshot> GetStatusAsync(
            ApplicationStatusRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Requests.Add(request);
            return Task.FromResult(CreateStatus(request.CodexHome));
        }
    }

    private sealed class CorruptOnCompleteLedger(string evidencePath) : IApplicationPlanLedger
    {
        private readonly InMemoryApplicationPlanLedger _inner = new();

        public Task RegisterAsync(
            ApplicationOperationPlan plan,
            CancellationToken cancellationToken = default)
        {
            return _inner.RegisterAsync(plan, cancellationToken);
        }

        public Task<ApplicationPlanClaimResult> TryClaimAsync(
            string planId,
            string digest,
            CancellationToken cancellationToken = default)
        {
            return _inner.TryClaimAsync(planId, digest, cancellationToken);
        }

        public Task CompleteAsync(
            string planId,
            ApplicationOperationLifecycle lifecycle,
            CancellationToken cancellationToken = default)
        {
            throw new ApplicationPlanLedgerCorruptionException(
                planId,
                evidencePath,
                "sensitive ledger detail");
        }
    }

    private sealed class TemporaryApplicationDirectory : IDisposable
    {
        public TemporaryApplicationDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                $"codex-provider-application-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose()
        {
            if (!Directory.Exists(Path))
            {
                return;
            }

            string fullPath = System.IO.Path.GetFullPath(Path);
            string fullTemp = System.IO.Path.GetFullPath(System.IO.Path.GetTempPath());
            string relative = System.IO.Path.GetRelativePath(fullTemp, fullPath);
            if (System.IO.Path.IsPathRooted(relative)
                || relative == ".."
                || relative.StartsWith($"..{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Refusing to remove a test directory outside the system temp root.");
            }

            Directory.Delete(fullPath, recursive: true);
        }
    }

    private sealed class FakeWritePort : IApplicationWritePort
    {
        public int PlanCalls { get; private set; }

        public int ExecuteCalls { get; private set; }

        public int SwitchExecuteCalls { get; private set; }

        public int RestoreExecuteCalls { get; private set; }

        public int PruneExecuteCalls { get; private set; }

        public ApplicationWriteIntent? LastIntent { get; private set; }

        public ApplicationOperationPlan? LastExecutedPlan { get; private set; }

        public List<ApplicationWriteIntent> ExecutedIntents { get; } = [];

        public SyncResult SyncResult { get; } = CreateSyncResult();

        public Func<ApplicationWriteIntent, ApplicationPlanPreview> PreviewFactory { get; set; }

        public Func<ApplicationWriteIntent, string, CancellationToken, Task<ApplicationPlanPreview>>? PreviewHandler { get; set; }

        public Func<SyncIntent, ApplicationOperationPlan, string, CancellationToken, Task<SyncResult>>? SyncHandler { get; set; }

        public FakeWritePort()
        {
            PreviewFactory = CreatePreview;
        }

        public ApplicationPlanPreview CreatePreview(ApplicationWriteIntent intent)
        {
            return new ApplicationPlanPreview(
                intent,
                $"state-{intent.Kind}",
                $"token-{intent.Kind}",
                [new ApplicationPlanTarget($"{intent.CodexHome}/target", "replace", "sha256:target")],
                intent is SyncIntent or SwitchIntent
                    ? [new ApplicationPlanTarget($"{intent.CodexHome}/backups/old", "delete", "sha256:backup")]
                    : [],
                []);
        }

        public Task<ApplicationPlanPreview> CreatePlanAsync(
            ApplicationWriteIntent intent,
            string operationId,
            CancellationToken cancellationToken = default)
        {
            PlanCalls++;
            return PreviewHandler is null
                ? Task.FromResult(PreviewFactory(intent))
                : PreviewHandler(intent, operationId, cancellationToken);
        }

        public Task<SyncResult> ExecuteSyncAsync(
            SyncIntent intent,
            ApplicationOperationPlan plan,
            string operationId,
            CancellationToken cancellationToken = default)
        {
            RecordExecution(intent, plan);
            return SyncHandler is null
                ? Task.FromResult(SyncResult)
                : SyncHandler(intent, plan, operationId, cancellationToken);
        }

        public Task<SyncResult> ExecuteSwitchAsync(
            SwitchIntent intent,
            ApplicationOperationPlan plan,
            string operationId,
            CancellationToken cancellationToken = default)
        {
            RecordExecution(intent, plan);
            SwitchExecuteCalls++;
            return Task.FromResult(SyncResult);
        }

        public Task<RestoreResult> ExecuteRestoreAsync(
            RestoreIntent intent,
            ApplicationOperationPlan plan,
            string operationId,
            CancellationToken cancellationToken = default)
        {
            RecordExecution(intent, plan);
            RestoreExecuteCalls++;
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
            RecordExecution(intent, plan);
            PruneExecuteCalls++;
            return Task.FromResult(new BackupPruneResult
            {
                BackupRoot = $"{intent.CodexHome}/backups",
                DeletedCount = 1,
                RemainingCount = intent.BackupRetentionCount,
                FreedBytes = 10
            });
        }

        private void RecordExecution(ApplicationWriteIntent intent, ApplicationOperationPlan plan)
        {
            ExecuteCalls++;
            LastIntent = intent;
            LastExecutedPlan = plan;
            ExecutedIntents.Add(intent);
        }
    }

    private static StatusSnapshot CreateStatus(string codexHome)
    {
        return new StatusSnapshot
        {
            CodexHome = codexHome,
            CurrentProvider = new CurrentProviderInfo("openai", false),
            ConfiguredProviders = ["openai", "relay"],
            RolloutCounts = new ProviderCounts(),
            LockedRolloutFiles = [],
            UnreadableRolloutFiles = [],
            EncryptedContentCounts = new ProviderCounts(),
            SqliteCounts = new ProviderCounts(),
            BackupRoot = $"{codexHome}/backups",
            BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
        };
    }

    private static SyncResult CreateSyncResult()
    {
        return new SyncResult
        {
            CodexHome = "/fixture",
            TargetProvider = "relay",
            PreviousProvider = "openai",
            BackupDir = "/fixture/backups/new",
            ChangedSessionFiles = 1,
            SkippedLockedRolloutFiles = [],
            SkippedUnreadableRolloutFiles = [],
            SqliteRowsUpdated = 1,
            SqlitePresent = true,
            RolloutCountsBefore = new ProviderCounts(),
            EncryptedContentCounts = new ProviderCounts()
        };
    }

    private static Dictionary<string, (long Length, long LastWrite)> SnapshotFiles(string root)
    {
        return Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
            .ToDictionary(
                path => Path.GetRelativePath(root, path),
                path =>
                {
                    FileInfo info = new(path);
                    return (info.Length, info.LastWriteTimeUtc.Ticks);
                },
                StringComparer.Ordinal);
    }
}
