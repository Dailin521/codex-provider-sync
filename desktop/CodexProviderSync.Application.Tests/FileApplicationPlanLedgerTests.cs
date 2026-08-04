using System.Diagnostics;
using System.Text;
using CodexProviderSync.Application;

namespace CodexProviderSync.Application.Tests;

public sealed class FileApplicationPlanLedgerTests
{
    private static readonly string DigestA = new('a', 64);
    private static readonly string DigestB = new('b', 64);
    private static readonly DateTimeOffset TestNow = new(2026, 8, 4, 3, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Constructor_RequiresAnExplicitFullyQualifiedNonRootPath_AndDoesNotTouchDisk()
    {
        using TemporaryDirectory temporary = new();
        string ledgerRoot = Path.Combine(temporary.Path, "not-created-yet");

        FileApplicationPlanLedger ledger = new(ledgerRoot);

        Assert.Equal(Path.GetFullPath(ledgerRoot), ledger.LedgerRoot);
        Assert.False(Directory.Exists(ledgerRoot));
        Assert.DoesNotContain(
            typeof(FileApplicationPlanLedger).GetConstructors(),
            static constructor => constructor.GetParameters().Length == 0);
        Assert.Throws<ArgumentException>(() => new FileApplicationPlanLedger(string.Empty));
        Assert.Throws<ArgumentException>(() => new FileApplicationPlanLedger("relative-ledger"));
        Assert.Throws<ArgumentException>(() => new FileApplicationPlanLedger(Path.GetPathRoot(ledgerRoot)!));
    }

    [Fact]
    public async Task Register_IsDurableCanonicalAndIdempotentAcrossIndependentInstances()
    {
        using TemporaryDirectory temporary = new();
        FakeTimeProvider time = new(TestNow);
        FileApplicationPlanLedger first = new(temporary.LedgerRoot, time);
        FileApplicationPlanLedger second = new(temporary.LedgerRoot, time);
        ApplicationOperationPlan plan = CreatePlan("plan-register", DigestA);

        await first.RegisterAsync(plan);
        await second.RegisterAsync(plan with { });

        string registrationPath = Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.registration.v1.json"));
        string json = await File.ReadAllTextAsync(registrationPath, Encoding.UTF8);
        Assert.EndsWith("\n", json, StringComparison.Ordinal);
        Assert.Contains("\"schemaVersion\":1", json, StringComparison.Ordinal);
        Assert.Contains("\"recordType\":\"registration\"", json, StringComparison.Ordinal);
        Assert.Contains("\"planId\":\"plan-register\"", json, StringComparison.Ordinal);
        Assert.Contains($"\"digest\":\"{DigestA}\"", json, StringComparison.Ordinal);
        Assert.True(json.IndexOf("\"schemaVersion\"", StringComparison.Ordinal)
            < json.IndexOf("\"recordType\"", StringComparison.Ordinal));
        Assert.True(json.IndexOf("\"recordType\"", StringComparison.Ordinal)
            < json.IndexOf("\"planId\"", StringComparison.Ordinal));
        Assert.True(json.IndexOf("\"planId\"", StringComparison.Ordinal)
            < json.IndexOf("\"digest\"", StringComparison.Ordinal));

        InvalidOperationException conflict = await Assert.ThrowsAsync<InvalidOperationException>(
            () => second.RegisterAsync(plan with { Digest = DigestB }));
        Assert.Contains("different digest", conflict.Message, StringComparison.Ordinal);
        Assert.Equal(json, await File.ReadAllTextAsync(registrationPath, Encoding.UTF8));
    }

    [Fact]
    public async Task TwentyIndependentInstances_AtomicallyAllowExactlyOneClaim()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-concurrent", DigestA);
        FileApplicationPlanLedger registrar = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await registrar.RegisterAsync(plan);

        FileApplicationPlanLedger[] contenders = Enumerable.Range(0, 20)
            .Select(index => new FileApplicationPlanLedger(
                temporary.LedgerRoot,
                new FakeTimeProvider(TestNow.AddMilliseconds(index))))
            .ToArray();
        TaskCompletionSource start = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<ApplicationPlanClaimResult>[] tasks = contenders
            .Select(async ledger =>
            {
                await start.Task;
                return await ledger.TryClaimAsync(plan.PlanId, plan.Digest);
            })
            .ToArray();

        start.SetResult();
        ApplicationPlanClaimResult[] claims = await Task.WhenAll(tasks);

        Assert.Single(claims, static claim => claim.Status == ApplicationPlanClaimStatus.Claimed);
        Assert.Equal(19, claims.Count(static claim => claim.Status == ApplicationPlanClaimStatus.AlreadyUsed));
        Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.claim.v1.json"));
    }

    [Fact]
    public async Task EightIndependentOsProcesses_AtomicallyAllowExactlyOneClaim()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-process-race", DigestA);
        FileApplicationPlanLedger registrar = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await registrar.RegisterAsync(plan);

        string configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent?.Name ?? "Debug";
        string testProjectDirectory = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
        string claimHostPath = Path.Combine(
            testProjectDirectory,
            "ClaimHost",
            "bin",
            configuration,
            "net10.0",
            "CodexProviderSync.PlanClaimHost.dll");
        Assert.True(File.Exists(claimHostPath), $"Plan-claim host was not built: {claimHostPath}");

        string startPath = Path.Combine(temporary.Path, "start.signal");
        string dotnetHost = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
        List<ChildProcess> children = [];
        try
        {
            for (int index = 0; index < 8; index++)
            {
                string readyPath = Path.Combine(temporary.Path, $"ready-{index}.signal");
                ProcessStartInfo startInfo = new(dotnetHost)
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                startInfo.ArgumentList.Add(claimHostPath);
                startInfo.ArgumentList.Add(temporary.LedgerRoot);
                startInfo.ArgumentList.Add(plan.PlanId);
                startInfo.ArgumentList.Add(plan.Digest);
                startInfo.ArgumentList.Add(readyPath);
                startInfo.ArgumentList.Add(startPath);
                Process process = Process.Start(startInfo)
                    ?? throw new InvalidOperationException("Failed to start a plan-claim child process.");
                children.Add(new ChildProcess(
                    process,
                    readyPath,
                    process.StandardOutput.ReadToEndAsync(),
                    process.StandardError.ReadToEndAsync()));
            }

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
            while (children.Any(static child => !File.Exists(child.ReadyPath)))
            {
                ChildProcess? exited = children.FirstOrDefault(static child => child.Process.HasExited);
                if (exited is not null)
                {
                    throw new InvalidOperationException(
                        $"A plan-claim child exited before the race barrier: {await exited.StandardError}");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
            }

            await File.WriteAllTextAsync(startPath, "start\n", timeout.Token);
            await Task.WhenAll(children.Select(child => child.Process.WaitForExitAsync(timeout.Token)));

            string[] statuses = await Task.WhenAll(children.Select(static child => child.StandardOutput));
            string[] errors = await Task.WhenAll(children.Select(static child => child.StandardError));
            for (int index = 0; index < children.Count; index++)
            {
                Assert.True(
                    children[index].Process.ExitCode == 0,
                    $"Plan-claim child {index} exited as {children[index].Process.ExitCode}: {errors[index]}");
            }

            Assert.Single(statuses, static status => status.Trim() == nameof(ApplicationPlanClaimStatus.Claimed));
            Assert.Equal(
                7,
                statuses.Count(static status => status.Trim() == nameof(ApplicationPlanClaimStatus.AlreadyUsed)));
            Assert.Single(Directory.EnumerateFiles(
                Path.Combine(temporary.LedgerRoot, "entries"),
                "*.claim.v1.json"));
        }
        finally
        {
            foreach (ChildProcess child in children)
            {
                try
                {
                    if (!child.Process.HasExited)
                    {
                        child.Process.Kill(entireProcessTree: true);
                        child.Process.WaitForExit();
                    }
                }
                catch (InvalidOperationException)
                {
                    // The child exited between the state check and cleanup.
                }
                child.Process.Dispose();
            }
        }
    }

    [Fact]
    public async Task ClaimedPlan_RemainsConsumedAfterProcessLikeRestartWithoutCompletion()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-crash", DigestA);
        FileApplicationPlanLedger beforeCrash = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await beforeCrash.RegisterAsync(plan);

        ApplicationPlanClaimResult claimed = await beforeCrash.TryClaimAsync(plan.PlanId, plan.Digest);
        Assert.Equal(ApplicationPlanClaimStatus.Claimed, claimed.Status);
        Assert.Empty(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.completion.v1.json"));

        FileApplicationPlanLedger afterRestart = new(
            temporary.LedgerRoot,
            new FakeTimeProvider(TestNow.AddMinutes(1)));
        ApplicationPlanClaimResult retried = await afterRestart.TryClaimAsync(plan.PlanId, plan.Digest);

        Assert.Equal(ApplicationPlanClaimStatus.AlreadyUsed, retried.Status);
        Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.claim.v1.json"));
    }

    [Fact]
    public async Task DigestMismatch_DoesNotConsumeTheRegisteredPlan()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-digest", DigestA);
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await ledger.RegisterAsync(plan);

        ApplicationPlanClaimResult mismatch = await ledger.TryClaimAsync(plan.PlanId, DigestB);
        Assert.Equal(ApplicationPlanClaimStatus.DigestMismatch, mismatch.Status);
        Assert.Empty(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.claim.v1.json"));

        ApplicationPlanClaimResult claimed = await ledger.TryClaimAsync(plan.PlanId, DigestA);
        Assert.Equal(ApplicationPlanClaimStatus.Claimed, claimed.Status);
    }

    [Fact]
    public async Task Complete_IsIdempotentForTheSameTerminalState_AndRejectsInvalidTransitions()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-complete", DigestA);
        FakeTimeProvider time = new(TestNow);
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, time);
        await ledger.RegisterAsync(plan);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Applying));

        await ledger.TryClaimAsync(plan.PlanId, plan.Digest);
        time.Advance(TimeSpan.FromSeconds(5));
        await ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded);
        await ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Succeeded);

        InvalidOperationException transition = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ledger.CompleteAsync(plan.PlanId, ApplicationOperationLifecycle.Failed));
        Assert.Contains("cannot transition", transition.Message, StringComparison.Ordinal);
        string completionPath = Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.completion.v1.json"));
        string completion = await File.ReadAllTextAsync(completionPath, Encoding.UTF8);
        Assert.Contains("\"lifecycle\":\"succeeded\"", completion, StringComparison.Ordinal);
        Assert.DoesNotContain("failed", completion, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CorruptRegistration_IsRejectedAndPreservedAsRecoveryEvidence()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-corrupt-registration", DigestA);
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await ledger.RegisterAsync(plan);
        string registrationPath = Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.registration.v1.json"));
        byte[] corrupted = Encoding.UTF8.GetBytes("{\"schemaVersion\":1");
        await File.WriteAllBytesAsync(registrationPath, corrupted);

        FileApplicationPlanLedger restarted = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        ApplicationPlanLedgerCorruptionException error =
            await Assert.ThrowsAsync<ApplicationPlanLedgerCorruptionException>(
                () => restarted.TryClaimAsync(plan.PlanId, plan.Digest));

        Assert.Equal("plan_ledger_corrupt", error.Code);
        Assert.Equal(plan.PlanId, error.PlanId);
        Assert.Equal(Path.GetFullPath(registrationPath), error.EvidencePath);
        Assert.Equal(corrupted, await File.ReadAllBytesAsync(registrationPath));
        Assert.Empty(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.claim.v1.json"));
    }

    [Fact]
    public async Task CorruptClaim_IsNeverTreatedAsReusable()
    {
        using TemporaryDirectory temporary = new();
        ApplicationOperationPlan plan = CreatePlan("plan-corrupt-claim", DigestA);
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        await ledger.RegisterAsync(plan);
        await ledger.TryClaimAsync(plan.PlanId, plan.Digest);
        string claimPath = Assert.Single(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.claim.v1.json"));
        await File.WriteAllTextAsync(claimPath, "{}\n", Encoding.UTF8);

        FileApplicationPlanLedger restarted = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        ApplicationPlanLedgerCorruptionException error =
            await Assert.ThrowsAsync<ApplicationPlanLedgerCorruptionException>(
                () => restarted.TryClaimAsync(plan.PlanId, plan.Digest));

        Assert.Equal(Path.GetFullPath(claimPath), error.EvidencePath);
        Assert.Empty(Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "entries"),
            "*.completion.v1.json"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("../escape")]
    [InlineData("..\\escape")]
    [InlineData("a/b")]
    [InlineData("a\\b")]
    [InlineData(".hidden")]
    [InlineData("-leading")]
    [InlineData("plan.with.dot")]
    [InlineData("计划")]
    public async Task UnsafePlanIds_AreRejectedBeforeStorageIsCreated(string planId)
    {
        using TemporaryDirectory temporary = new();
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));

        await Assert.ThrowsAsync<ArgumentException>(
            () => ledger.TryClaimAsync(planId, DigestA));

        Assert.False(Directory.Exists(temporary.LedgerRoot));
    }

    [Fact]
    public async Task LocksArePerPlanAndPersistent_WithoutDeletingAnotherPlansLock()
    {
        using TemporaryDirectory temporary = new();
        FileApplicationPlanLedger ledger = new(temporary.LedgerRoot, new FakeTimeProvider(TestNow));
        ApplicationOperationPlan first = CreatePlan("plan-lock-one", DigestA);
        ApplicationOperationPlan second = CreatePlan("plan-lock-two", DigestB);

        await ledger.RegisterAsync(first);
        await ledger.RegisterAsync(second);
        await ledger.TryClaimAsync(first.PlanId, first.Digest);
        await ledger.TryClaimAsync(second.PlanId, second.Digest);

        string[] lockFiles = Directory.EnumerateFiles(
            Path.Combine(temporary.LedgerRoot, "locks"),
            "*.lock").ToArray();
        Assert.Equal(2, lockFiles.Length);
        Assert.All(lockFiles, static path => Assert.True(File.Exists(path)));
    }

    private static ApplicationOperationPlan CreatePlan(string planId, string digest)
    {
        return new ApplicationOperationPlan(
            ApplicationProtocol.Version,
            planId,
            $"operation-{planId}",
            TestNow,
            TestNow.AddMinutes(10),
            new SyncIntent("/isolated/fixture", null, "relay"),
            "state-fingerprint",
            "execution-token",
            [new ApplicationPlanTarget("/isolated/fixture/session.jsonl", "replace", "sha256:target")],
            [],
            [],
            digest);
    }

    private sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan duration) => _now = _now.Add(duration);
    }

    private sealed record ChildProcess(
        Process Process,
        string ReadyPath,
        Task<string> StandardOutput,
        Task<string> StandardError);

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                $"codex-provider-plan-ledger-tests-{Guid.NewGuid():N}");
            LedgerRoot = System.IO.Path.Combine(Path, "ledger");
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public string LedgerRoot { get; }

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
}
