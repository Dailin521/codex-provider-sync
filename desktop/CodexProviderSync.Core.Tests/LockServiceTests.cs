using System.Diagnostics;
using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class LockServiceTests
{
    private const int NodeBusyExitCode = 73;
    private static readonly TimeSpan NodeProcessTimeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task AcquireLockAsync_PublishesVersionedOwnerAndClaim_ThenReleasesOnlyItsGeneration()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";

        LockHandle handle = await new LockService().AcquireLockAsync(codexHome, "test");
        try
        {
            Assert.Equal(Path.GetFullPath(lockPath), handle.LockPath);
            Assert.True(Directory.Exists(lockPath));
            string claimPath = Assert.Single(Directory.EnumerateFiles(claimsPath, "*.json"));

            using JsonDocument owner = JsonDocument.Parse(
                await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
            JsonElement root = owner.RootElement;
            Assert.Equal(2, root.GetProperty("protocolVersion").GetInt32());
            Assert.Equal("dotnet", root.GetProperty("runtime").GetString());
            Assert.Equal(Environment.ProcessId, root.GetProperty("pid").GetInt32());
            Assert.Equal(Environment.ProcessId, root.GetProperty("processId").GetInt32());
            Assert.Equal(handle.InstanceId, root.GetProperty("instanceId").GetString());
            Assert.Equal("test", root.GetProperty("label").GetString());
            Assert.Equal(Environment.CurrentDirectory, root.GetProperty("cwd").GetString());
            Assert.Matches(
                @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
                root.GetProperty("processStartedAt").GetString()!);

            using JsonDocument claim = JsonDocument.Parse(await File.ReadAllTextAsync(claimPath));
            Assert.Equal(handle.InstanceId, claim.RootElement.GetProperty("instanceId").GetString());
            string reservationMarker = Assert.Single(
                Directory.EnumerateFiles(lockPath, ".reservation.*", SearchOption.TopDirectoryOnly));
            Assert.Equal(handle.InstanceId, await File.ReadAllTextAsync(reservationMarker));
        }
        finally
        {
            await handle.DisposeAsync();
        }

        Assert.False(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(claimsPath, "*.json"));
    }

    [Fact]
    public async Task AcquirePathLockAsync_SupportsArbitraryExplicitResourcePath()
    {
        string root = CreateTempDirectory();
        string lockPath = Path.Combine(root, "resource-locks", "sqlite-home.lock");

        await using (LockHandle handle = await new LockService().AcquirePathLockAsync(lockPath, "sqlite"))
        {
            Assert.Equal(Path.GetFullPath(lockPath), handle.LockPath);
            Assert.True(Directory.Exists(lockPath));
            Assert.True(Directory.Exists(lockPath + ".claims"));
        }

        Assert.False(Directory.Exists(lockPath));
    }

    [Fact]
    public async Task AcquirePathLockAsync_RejectsCanonicalFileWithBusyDiagnostic()
    {
        string root = CreateTempDirectory();
        string lockPath = Path.Combine(root, "resource-locks", "sqlite-home.lock");
        Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);
        await File.WriteAllTextAsync(lockPath, "foreign");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquirePathLockAsync(lockPath, "sqlite"));

        Assert.Contains("not a directory", error.Message);
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal("foreign", await File.ReadAllTextAsync(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquirePathLockAsync_RejectsCanonicalSymbolicLinkWithoutFollowingIt_OnUnix()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }
        string root = CreateTempDirectory();
        string lockPath = Path.Combine(root, "resource-locks", "sqlite-home.lock");
        string targetPath = Path.Combine(root, "foreign-target");
        Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);
        Directory.CreateDirectory(targetPath);
        Directory.CreateSymbolicLink(lockPath, targetPath);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquirePathLockAsync(lockPath, "sqlite"));

        Assert.Contains("symbolic link or reparse point", error.Message);
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Empty(Directory.EnumerateFileSystemEntries(targetPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_HardLinkFailurePreservesForeignPopulationAndReleasesClaim()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        LockService service = new(async (phase, _) =>
        {
            if (phase != "canonical-reserved")
            {
                return;
            }
            await File.WriteAllTextAsync(Path.Combine(lockPath, "owner.json"), "foreign-owner");
            await File.WriteAllTextAsync(Path.Combine(lockPath, "foreign.txt"), "keep");
        });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireLockAsync(codexHome, "injected"));

        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal("foreign-owner", await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
        Assert.Equal("keep", await File.ReadAllTextAsync(Path.Combine(lockPath, "foreign.txt")));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_ReservationAbaRetainsUncertainClaimAndForeignDirectory()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string displacedPath = lockPath + ".displaced";
        LockService service = new(async (phase, _) =>
        {
            if (phase != "canonical-reserved")
            {
                return;
            }
            Directory.Move(lockPath, displacedPath);
            Directory.CreateDirectory(lockPath);
            await File.WriteAllTextAsync(Path.Combine(lockPath, "foreign.txt"), "keep");
        });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireLockAsync(codexHome, "aba"));

        Assert.Contains("reservation changed identity", error.Message);
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal("keep", await File.ReadAllTextAsync(Path.Combine(lockPath, "foreign.txt")));
        Assert.Single(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
        Assert.True(Directory.Exists(displacedPath));
    }

    [Fact]
    public async Task AcquireLockAsync_ReclaimsCanonicalOwnedByExitedProcess()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            processId = int.MaxValue,
            startedAt = DateTimeOffset.UtcNow.AddHours(-1),
            processStartedAt = "2000-01-01T00:00:00Z",
            label = "crashed",
            currentDirectory = codexHome
        });

        await using (LockHandle handle = await new LockService().AcquireLockAsync(codexHome, "recovery"))
        {
            using JsonDocument owner = JsonDocument.Parse(
                await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
            Assert.Equal(handle.InstanceId, owner.RootElement.GetProperty("instanceId").GetString());
        }

        Assert.False(Directory.Exists(lockPath));
    }

    [Fact]
    public async Task AcquireLockAsync_DoesNotReclaimLiveLegacyDotNetOwner()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            processId = Environment.ProcessId,
            startedAt = DateTimeOffset.UtcNow,
            processStartedAt = LockService.CurrentProcessStartedAtForTests(),
            label = "active-legacy-dotnet",
            currentDirectory = codexHome
        });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.Contains("verified owner", error.Message);
        Assert.True(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_DoesNotReclaimLiveLegacyNodeOwner()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            pid = Environment.ProcessId,
            processStartedAt = LockService.CurrentProcessStartedAtForTests(),
            instanceId = Guid.NewGuid().ToString("D"),
            runtime = "node",
            label = "active-legacy-node",
            cwd = codexHome
        });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.True(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_DoesNotReclaimLiveLegacyNodeMarkerOwner()
    {
        string? marker = LockService.CurrentProcessStartMarkerForTests();
        if (marker is null)
        {
            return;
        }
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            pid = Environment.ProcessId,
            processStartMarker = marker,
            instanceId = Guid.NewGuid().ToString("D"),
            runtime = "node",
            label = "active-legacy-node-marker",
            cwd = codexHome
        });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.True(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_FailsClosedForOwnerlessLegacyLock()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.Contains("retained fail-closed", error.Message);
        Assert.True(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_ReclaimsCanonicalAndClaimWhenPidWasReused()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string staleInstanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(lockPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(lockPath, "owner.json"),
            staleInstanceId,
            "2000-01-01T00:00:00Z");
        string claimsPath = lockPath + ".claims";
        Directory.CreateDirectory(claimsPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(claimsPath, staleInstanceId + ".json"),
            staleInstanceId,
            "2000-01-01T00:00:00Z");

        await using (LockHandle handle = await new LockService().AcquireLockAsync(codexHome, "replacement"))
        {
            Assert.NotEqual(staleInstanceId, handle.InstanceId);
            Assert.DoesNotContain(
                Directory.EnumerateFiles(claimsPath, "*.json"),
                path => string.Equals(
                    Path.GetFileNameWithoutExtension(path),
                    staleInstanceId,
                    StringComparison.OrdinalIgnoreCase));
        }
        Assert.False(Directory.Exists(lockPath));
    }

    [Fact]
    public async Task AcquireLockAsync_TwoReclaimersCannotBothOwnStaleCanonicalGeneration()
    {
        await AssertTwoReclaimersRoundAsync(iteration: 0);
    }

    [Fact]
    public async Task AcquireLockAsync_TwoReclaimersStressLeavesNoLiveClaim()
    {
        for (int iteration = 1; iteration <= 50; iteration += 1)
        {
            await AssertTwoReclaimersRoundAsync(iteration);
        }
    }

    [Fact]
    public async Task AcquireLockAsync_RetriesTransientOwnedClaimCleanupFailure()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        string liveInstanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(claimsPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(claimsPath, liveInstanceId + ".json"),
            liveInstanceId,
            LockService.CurrentProcessStartedAtForTests());
        int cleanupAttempts = 0;
        LockService service = new((phase, _) =>
        {
            if (phase == "before-owned-claim-delete"
                && Interlocked.Increment(ref cleanupAttempts) < 3)
            {
                throw new IOException("injected transient owned-claim cleanup failure");
            }
            return Task.CompletedTask;
        });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireLockAsync(codexHome, "cleanup-retry"));

        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal(3, cleanupAttempts);
        Assert.False(Directory.Exists(lockPath));
        Assert.Equal(
            [liveInstanceId + ".json"],
            Directory.EnumerateFiles(claimsPath, "*.json").Select(path => Path.GetFileName(path)!).ToArray());
    }

    [Fact]
    public async Task AcquireLockAsync_AggregatesAcquisitionAndExhaustedOwnedClaimCleanupFailure()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        string liveInstanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(claimsPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(claimsPath, liveInstanceId + ".json"),
            liveInstanceId,
            LockService.CurrentProcessStartedAtForTests());
        int cleanupAttempts = 0;
        LockService service = new((phase, _) =>
        {
            if (phase == "before-owned-claim-delete")
            {
                int attempt = Interlocked.Increment(ref cleanupAttempts);
                throw new IOException($"injected owned-claim cleanup failure {attempt}");
            }
            return Task.CompletedTask;
        });

        AggregateException error = await Assert.ThrowsAsync<AggregateException>(
            () => service.AcquireLockAsync(codexHome, "cleanup-exhausted"));

        Assert.Equal(4, cleanupAttempts);
        Assert.Contains("Lock already exists", error.Message);
        Assert.Contains("after 4 attempts", error.Message);
        Assert.False(LockService.IsOperationBusy(error));
        Assert.Contains(
            error.InnerExceptions,
            LockService.IsOperationBusy);
        Assert.Contains(
            error.InnerExceptions,
            inner => inner is IOException
                && inner.ToString().Contains("injected owned-claim cleanup failure 4", StringComparison.Ordinal));
        Assert.False(Directory.Exists(lockPath));
        Assert.Equal(2, Directory.EnumerateFiles(claimsPath, "*.json").Count());
    }

    [Fact]
    public async Task AcquireLockAsync_RevalidatesOwnedClaimIdentityBeforeCleanupRetry()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        string liveInstanceId = Guid.NewGuid().ToString("D");
        string replacementInstanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(claimsPath);
        string liveClaimPath = Path.Combine(claimsPath, liveInstanceId + ".json");
        await WriteVersionTwoOwnerAsync(
            liveClaimPath,
            liveInstanceId,
            LockService.CurrentProcessStartedAtForTests());
        int cleanupAttempts = 0;
        LockService service = new(async (phase, instanceId) =>
        {
            if (phase == "before-owned-claim-delete")
            {
                Interlocked.Increment(ref cleanupAttempts);
                string claimPath = Path.Combine(claimsPath, instanceId + ".json");
                await WriteVersionTwoOwnerAsync(
                    claimPath,
                    replacementInstanceId,
                    LockService.CurrentProcessStartedAtForTests());
                throw new IOException("retry after replacing claim identity");
            }
        });

        AggregateException error = await Assert.ThrowsAsync<AggregateException>(
            () => service.AcquireLockAsync(codexHome, "cleanup-revalidation"));

        Assert.Equal(1, cleanupAttempts);
        Assert.Contains("Lock already exists", error.Message);
        Assert.Contains(replacementInstanceId, error.Message);
        Assert.False(LockService.IsOperationBusy(error));
        Assert.Contains(error.InnerExceptions, LockService.IsOperationBusy);
        string retainedClaim = Assert.Single(
            Directory.EnumerateFiles(claimsPath, "*.json"),
            path => !string.Equals(path, liveClaimPath, StringComparison.OrdinalIgnoreCase));
        using JsonDocument owner = JsonDocument.Parse(await File.ReadAllTextAsync(retainedClaim));
        Assert.Equal(replacementInstanceId, owner.RootElement.GetProperty("instanceId").GetString());
    }

    [Fact]
    public async Task IdentityReadStream_AllowsConcurrentDelete()
    {
        string directory = CreateTempDirectory();
        string identityPath = Path.Combine(directory, "owner.json");
        await File.WriteAllTextAsync(identityPath, "{}");

        await using FileStream identityRead = LockService.OpenIdentityReadStreamForTests(identityPath);
        Assert.True(identityRead.CanRead);
        File.Delete(identityPath);

        Assert.False(File.Exists(identityPath));
    }

    [Fact]
    public async Task DisposeAsync_DoesNotDeleteReplacementCanonicalOwner_AbaDefense()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        LockHandle original = await new LockService().AcquireLockAsync(codexHome, "original");
        string replacementInstanceId = Guid.NewGuid().ToString("D");
        await WriteVersionTwoOwnerAsync(
            Path.Combine(lockPath, "owner.json"),
            replacementInstanceId,
            LockService.CurrentProcessStartedAtForTests());

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => original.DisposeAsync().AsTask());

        Assert.Contains("owner identity changed", error.Message);
        Assert.True(Directory.Exists(lockPath));
        using JsonDocument owner = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
        Assert.Equal(replacementInstanceId, owner.RootElement.GetProperty("instanceId").GetString());
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task DisposeAsync_DoesNotDeleteReplacementDirectoryThatReusesOwnerFile()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        LockHandle original = await new LockService().AcquireLockAsync(codexHome, "original");
        string originalPath = lockPath + ".original";
        Directory.Move(lockPath, originalPath);
        Directory.CreateDirectory(lockPath);
        File.Copy(
            Path.Combine(originalPath, "owner.json"),
            Path.Combine(lockPath, "owner.json"));
        await File.WriteAllTextAsync(Path.Combine(lockPath, "foreign.txt"), "keep");

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => original.DisposeAsync().AsTask());

        Assert.Contains("reservation identity changed", error.Message);
        Assert.Equal("keep", await File.ReadAllTextAsync(Path.Combine(lockPath, "foreign.txt")));
        Assert.Single(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_StaleReclaimAbaRestoresAndPreservesReplacementOwner()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string oldInstanceId = Guid.NewGuid().ToString("D");
        string replacementInstanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(lockPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(lockPath, "owner.json"),
            oldInstanceId,
            "2000-01-01T00:00:00Z");
        string releasedOldPath = lockPath + ".released-old";
        LockService service = new(async (phase, _) =>
        {
            if (phase != "before-stale-canonical-reclaim")
            {
                return;
            }
            Directory.Move(lockPath, releasedOldPath);
            Directory.CreateDirectory(lockPath);
            await WriteVersionTwoOwnerAsync(
                Path.Combine(lockPath, "owner.json"),
                replacementInstanceId,
                LockService.CurrentProcessStartedAtForTests());
        });

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireLockAsync(codexHome, "aba-contender"));

        Assert.Contains("owner changed during reclamation", error.Message);
        Assert.True(Directory.Exists(lockPath));
        using JsonDocument owner = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
        Assert.Equal(replacementInstanceId, owner.RootElement.GetProperty("instanceId").GetString());
        Assert.True(Directory.Exists(releasedOldPath));
        Assert.Contains(
            Directory.EnumerateDirectories(Path.GetDirectoryName(lockPath)!),
            path => Path.GetFileName(path).Contains(".stale.", StringComparison.Ordinal));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_FailsClosedForLiveVersionTwoClaimWithoutTouchingCanonical()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        string liveInstance = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(claimsPath);
        await WriteVersionTwoOwnerAsync(
            Path.Combine(claimsPath, liveInstance + ".json"),
            liveInstance,
            LockService.CurrentProcessStartedAtForTests());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.False(Directory.Exists(lockPath));
        Assert.Equal(
            [liveInstance + ".json"],
            Directory.EnumerateFiles(claimsPath, "*.json").Select(path => Path.GetFileName(path)!).ToArray());
    }

    [Fact]
    public async Task AcquireLockAsync_FailsClosedForConflictingPidFieldsInVersionTwoClaim()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        string instanceId = Guid.NewGuid().ToString("D");
        Directory.CreateDirectory(claimsPath);
        string claimPath = Path.Combine(claimsPath, instanceId + ".json");
        await WriteJsonAsync(claimPath, new
        {
            protocolVersion = 2,
            runtime = "node",
            pid = Environment.ProcessId,
            processId = int.MaxValue,
            processStartedAt = LockService.CurrentProcessStartedAtForTests(),
            instanceId,
            label = "conflicting-schema",
            cwd = Environment.CurrentDirectory
        });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.True(File.Exists(claimPath));
        Assert.False(Directory.Exists(lockPath));
    }

    [Fact]
    public async Task AcquireLockAsync_FailsClosedForFutureCanonicalProtocol()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            protocolVersion = 99,
            pid = int.MaxValue,
            processId = int.MaxValue,
            processStartedAt = "2000-01-01T00:00:00Z",
            instanceId = "future-owner",
            label = "future",
            cwd = Environment.CurrentDirectory
        });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.True(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(lockPath + ".claims", "*.json"));
    }

    [Fact]
    public async Task AcquireLockAsync_FailsClosedWhenClaimFilenameDoesNotMatchInstanceId()
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        Directory.CreateDirectory(claimsPath);
        string mismatchedPath = Path.Combine(claimsPath, Guid.NewGuid().ToString("D") + ".json");
        await WriteVersionTwoOwnerAsync(
            mismatchedPath,
            "opaque-node-instance-id",
            LockService.CurrentProcessStartedAtForTests());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new LockService().AcquireLockAsync(codexHome, "competing"));

        Assert.True(File.Exists(mismatchedPath));
        Assert.False(Directory.Exists(lockPath));
    }

    [Fact]
    public async Task AcquireLockAsync_RealNodeOwnerBlocksDotNetThenReleases_OnLinux()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        Process? node = null;
        try
        {
            node = StartNodeLockHelper("hold", codexHome);
            using JsonDocument acquired = await ReadNodeEventAsync(node, "acquired");
            Assert.Equal(node.Id, acquired.RootElement.GetProperty("pid").GetInt32());

            string ownerBefore = await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json"));
            string[] claimsBefore = SnapshotClaimFiles(lockPath);
            using (JsonDocument owner = JsonDocument.Parse(ownerBefore))
            {
                Assert.Equal("node", owner.RootElement.GetProperty("runtime").GetString());
                Assert.Equal(node.Id, owner.RootElement.GetProperty("pid").GetInt32());
            }

            InvalidOperationException blocked = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new LockService().AcquireLockAsync(codexHome, "dotnet-contender"));

            Assert.True(LockService.IsOperationBusy(blocked));
            Assert.Equal(ownerBefore, await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
            Assert.Equal(claimsBefore, SnapshotClaimFiles(lockPath));

            await node.StandardInput.WriteLineAsync("release");
            node.StandardInput.Close();
            NodeHelperResult released = await CompleteNodeHelperAsync(node);
            AssertNodeExit(released, expectedExitCode: 0);
            Assert.Contains("\"event\":\"released\"", released.Stdout);

            await using (LockHandle dotnet = await new LockService().AcquireLockAsync(
                codexHome,
                "dotnet-after-node"))
            {
                Assert.True(Directory.Exists(lockPath));
            }
            Assert.False(Directory.Exists(lockPath));
            Assert.Empty(SnapshotClaimFiles(lockPath));
        }
        finally
        {
            if (node is not null)
            {
                await StopNodeHelperAsync(node);
                node.Dispose();
            }
            TryDeleteTempDirectory(codexHome);
        }
    }

    [Fact]
    public async Task AcquireLockAsync_RealDotNetOwnerBlocksNodeThenReleases_OnLinux()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        try
        {
            await using (LockHandle dotnet = await new LockService().AcquireLockAsync(
                codexHome,
                "dotnet-holder"))
            {
                string ownerBefore = await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json"));
                string[] claimsBefore = SnapshotClaimFiles(lockPath);
                using (JsonDocument owner = JsonDocument.Parse(ownerBefore))
                {
                    Assert.Equal("dotnet", owner.RootElement.GetProperty("runtime").GetString());
                    Assert.Equal(Environment.ProcessId, owner.RootElement.GetProperty("pid").GetInt32());
                }

                NodeHelperResult blocked = await RunNodeLockHelperAsync("attempt", codexHome);

                AssertNodeExit(blocked, NodeBusyExitCode);
                Assert.Contains("\"event\":\"busy\"", blocked.Stdout);
                Assert.Equal(ownerBefore, await File.ReadAllTextAsync(Path.Combine(lockPath, "owner.json")));
                Assert.Equal(claimsBefore, SnapshotClaimFiles(lockPath));
            }

            NodeHelperResult acquired = await RunNodeLockHelperAsync("attempt", codexHome);
            AssertNodeExit(acquired, expectedExitCode: 0);
            Assert.Contains("\"event\":\"acquired\"", acquired.Stdout);
            Assert.Contains("\"event\":\"released\"", acquired.Stdout);
            Assert.False(Directory.Exists(lockPath));
            Assert.Empty(SnapshotClaimFiles(lockPath));
        }
        finally
        {
            TryDeleteTempDirectory(codexHome);
        }
    }

    [Fact]
    public async Task CreateLockDirectoryAsync_RetriesTransientAccessDeniedErrors()
    {
        int attempts = 0;
        List<int> delays = [];

        await LockService.CreateLockDirectoryAsync(
            @"C:\temp\provider-sync.lock",
            retryCount: 3,
            retryDelayMs: 75,
            delayAsync: delay =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            },
            tryCreateDirectory: _ =>
            {
                attempts += 1;
                return attempts < 3 ? 5 : 0;
            });

        Assert.Equal(3, attempts);
        Assert.Equal([75, 75], delays);
    }

    [Fact]
    public async Task CreateLockDirectoryAsync_ThrowsWhenLockAlreadyExists()
    {
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => LockService.CreateLockDirectoryAsync(
                @"C:\temp\provider-sync.lock",
                tryCreateDirectory: _ => 183));

        Assert.Contains("Lock already exists", error.Message);
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal(
            LockService.OperationBusyErrorCode,
            error.Data["codex-provider-sync/error-code"]);
    }

    [Fact]
    public async Task CreateLockDirectoryAsync_ThrowsAfterTransientRetryBudgetIsExhausted()
    {
        IOException error = await Assert.ThrowsAsync<IOException>(
            () => LockService.CreateLockDirectoryAsync(
                @"C:\temp\provider-sync.lock",
                retryCount: 2,
                retryDelayMs: 10,
                delayAsync: _ => Task.CompletedTask,
                tryCreateDirectory: _ => 5));

        Assert.Contains("Win32 error: 5", error.Message);
    }

    private static async Task<LockHandle?> TryAcquireAsync(LockService service, string codexHome)
    {
        try
        {
            return await service.AcquireLockAsync(codexHome, "contender");
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private static async Task AssertTwoReclaimersRoundAsync(int iteration)
    {
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
        string claimsPath = lockPath + ".claims";
        Directory.CreateDirectory(lockPath);
        await WriteJsonAsync(Path.Combine(lockPath, "owner.json"), new
        {
            processId = int.MaxValue,
            processStartedAt = "2000-01-01T00:00:00Z",
            label = "stale",
            currentDirectory = codexHome
        });

        int published = 0;
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<string, string, Task> hook = (phase, _) =>
        {
            if (phase == "claim-published" && Interlocked.Increment(ref published) == 2)
            {
                gate.TrySetResult();
            }
            return gate.Task;
        };

        Task<LockHandle?> first = TryAcquireAsync(new LockService(hook), codexHome);
        Task<LockHandle?> second = TryAcquireAsync(new LockService(hook), codexHome);
        LockHandle?[] acquired = await Task.WhenAll(first, second);
        LockHandle[] winners = acquired.OfType<LockHandle>().ToArray();
        Assert.True(
            winners.Length <= 1,
            $"Two concurrent stale-lock reclaimers both acquired the canonical lock in stress iteration {iteration}.");

        foreach (LockHandle winner in winners)
        {
            await winner.DisposeAsync();
        }

        await using (LockHandle retry = await new LockService().AcquireLockAsync(codexHome, "retry"))
        {
            Assert.True(Directory.Exists(lockPath));
        }
        Assert.False(Directory.Exists(lockPath));
        Assert.Empty(Directory.EnumerateFiles(claimsPath, "*.json"));
    }

    private static string CreateTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"codex-provider-lock-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static Process StartNodeLockHelper(string mode, string codexHome)
    {
        string helperPath = Path.Combine(FindRepositoryRoot(), "test", "helpers", "lock-contender.js");
        ProcessStartInfo startInfo = new()
        {
            FileName = Environment.GetEnvironmentVariable("NODE_BINARY") ?? "node",
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        startInfo.ArgumentList.Add(helperPath);
        startInfo.ArgumentList.Add(mode);
        startInfo.ArgumentList.Add(codexHome);

        Process process = new() { StartInfo = startInfo };
        if (!process.Start())
        {
            process.Dispose();
            throw new InvalidOperationException("Failed to start the Node lock contender helper.");
        }
        return process;
    }

    private static async Task<JsonDocument> ReadNodeEventAsync(Process process, string expectedEvent)
    {
        string? line = await process.StandardOutput.ReadLineAsync().WaitAsync(NodeProcessTimeout);
        if (line is null)
        {
            string stderr = await process.StandardError.ReadToEndAsync();
            throw new InvalidOperationException(
                $"Node lock contender exited before event '{expectedEvent}'. stderr: {stderr}");
        }

        JsonDocument document = JsonDocument.Parse(line);
        string? actualEvent = document.RootElement.GetProperty("event").GetString();
        if (!string.Equals(actualEvent, expectedEvent, StringComparison.Ordinal))
        {
            document.Dispose();
            throw new InvalidOperationException(
                $"Expected Node lock event '{expectedEvent}', received '{actualEvent}': {line}");
        }
        return document;
    }

    private static async Task<NodeHelperResult> RunNodeLockHelperAsync(string mode, string codexHome)
    {
        using Process process = StartNodeLockHelper(mode, codexHome);
        process.StandardInput.Close();
        return await CompleteNodeHelperAsync(process);
    }

    private static async Task<NodeHelperResult> CompleteNodeHelperAsync(Process process)
    {
        Task<string> stdout = process.StandardOutput.ReadToEndAsync();
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync().WaitAsync(NodeProcessTimeout);
        }
        catch (TimeoutException)
        {
            await StopNodeHelperAsync(process);
            throw new TimeoutException(
                $"Node lock contender did not exit within {NodeProcessTimeout.TotalSeconds:F0} seconds.");
        }
        return new NodeHelperResult(process.ExitCode, await stdout, await stderr);
    }

    private static async Task StopNodeHelperAsync(Process process)
    {
        if (process.HasExited)
        {
            return;
        }

        try
        {
            await process.StandardInput.WriteLineAsync("release");
            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(3));
        }
        catch
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(3));
            }
        }
    }

    private static string[] SnapshotClaimFiles(string lockPath)
    {
        string claimsPath = lockPath + ".claims";
        return Directory.Exists(claimsPath)
            ? Directory.EnumerateFiles(claimsPath, "*.json", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .OfType<string>()
                .Order(StringComparer.Ordinal)
                .ToArray()
            : [];
    }

    private static void AssertNodeExit(NodeHelperResult result, int expectedExitCode)
    {
        Assert.True(
            result.ExitCode == expectedExitCode,
            $"Expected Node helper exit code {expectedExitCode}, received {result.ExitCode}. "
            + $"stdout: {result.Stdout} stderr: {result.Stderr}");
    }

    private static string FindRepositoryRoot()
    {
        foreach (string start in new[] { Environment.CurrentDirectory, AppContext.BaseDirectory })
        {
            DirectoryInfo? directory = new(Path.GetFullPath(start));
            while (directory is not null)
            {
                if (File.Exists(Path.Combine(directory.FullName, "package.json"))
                    && File.Exists(Path.Combine(
                        directory.FullName,
                        "test",
                        "helpers",
                        "lock-contender.js")))
                {
                    return directory.FullName;
                }
                directory = directory.Parent;
            }
        }
        throw new DirectoryNotFoundException(
            "Could not locate the repository root for test/helpers/lock-contender.js.");
    }

    private static void TryDeleteTempDirectory(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; assertions report any retained lock state first.
        }
    }

    private sealed record NodeHelperResult(int ExitCode, string Stdout, string Stderr);

    private static Task WriteJsonAsync(string path, object value)
    {
        return File.WriteAllTextAsync(path, JsonSerializer.Serialize(value));
    }

    private static Task WriteVersionTwoOwnerAsync(
        string path,
        string instanceId,
        string processStartedAt)
    {
        return WriteJsonAsync(path, new
        {
            protocolVersion = 2,
            runtime = "node",
            pid = Environment.ProcessId,
            processId = Environment.ProcessId,
            processStartedAt,
            instanceId,
            startedAt = DateTimeOffset.UtcNow,
            label = "fixture",
            cwd = Environment.CurrentDirectory
        });
    }
}
