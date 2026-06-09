namespace CodexProviderSync.Core.Tests;

public sealed class LockServiceTests
{
    [Fact]
    public async Task AcquireLockAsync_CreatesAndReleasesLockDirectory()
    {
        string codexHome = Path.Combine(Path.GetTempPath(), $"codex-provider-lock-{Guid.NewGuid():N}");
        Directory.CreateDirectory(codexHome);
        string lockPath = AppConstants.LockPath(codexHome);

        await using (await new LockService().AcquireLockAsync(codexHome, "test"))
        {
            Assert.True(Directory.Exists(lockPath));
            Assert.True(File.Exists(Path.Combine(lockPath, "owner.json")));
        }

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
}
