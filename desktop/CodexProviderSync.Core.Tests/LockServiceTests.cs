using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class LockServiceTests
{
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
        string codexHome = CreateTempDirectory();
        string lockPath = AppConstants.LockPath(codexHome);
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
        Assert.True(winners.Length <= 1, "Two concurrent stale-lock reclaimers both acquired the canonical lock.");

        foreach (LockHandle winner in winners)
        {
            await winner.DisposeAsync();
        }

        await using LockHandle retry = await new LockService().AcquireLockAsync(codexHome, "retry");
        Assert.True(Directory.Exists(lockPath));
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

    private static string CreateTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"codex-provider-lock-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

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
