using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CodexProviderSync.Core;

namespace CodexProviderSync.Core.Tests;

public sealed class StateDbLockResourceTests
{
    [Fact]
    public async Task ResolveAsync_UsesNulDelimitedPhysicalIdentityAndLowerSha256()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "sqlite");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");

        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(stateDbPath);
        string parent = resource.RealDbParent;
        string expectedIdentity = (OperatingSystem.IsWindows() ? parent.ToLowerInvariant() : parent)
            + "\0"
            + (OperatingSystem.IsWindows() ? AppConstants.DbFileBasename.ToLowerInvariant() : AppConstants.DbFileBasename);
        string expectedKey = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(expectedIdentity))).ToLowerInvariant();

        Assert.Equal(expectedIdentity, resource.Identity);
        Assert.Equal(expectedKey, resource.ResourceKey);
        Assert.Equal(
            Path.Combine(parent, ".codex-provider-sync", "locks", expectedKey + ".lock"),
            resource.LockPath);
    }

    [Fact]
    public async Task AcquireStateDbLockAsync_PublishesResourceFieldsAndReportsVerifiedBusy()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "sqlite");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");
        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(stateDbPath);
        LockService service = new();

        await using LockHandle first = await service.AcquireStateDbLockAsync(resource, "first");
        using JsonDocument owner = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(resource.LockPath, "owner.json")));
        Assert.Equal("state-db", owner.RootElement.GetProperty("scope").GetString());
        Assert.Equal(resource.ResourceKey, owner.RootElement.GetProperty("resourceKey").GetString());

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireStateDbLockAsync(resource, "second"));
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal("state-db", error.Data["codex-provider-sync/lock-scope"]);
        Assert.Equal(resource.ResourceKey, error.Data["codex-provider-sync/resource-key"]);
    }

    [Fact]
    public async Task ResolveAsync_AllowsMissingDatabaseOnlyWithVerifiedParent()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "sqlite");
        Directory.CreateDirectory(sqliteHome);
        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(
            Path.Combine(sqliteHome, AppConstants.DbFileBasename));
        Assert.Matches("^[a-f0-9]{64}$", resource.ResourceKey);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => StateDbLockResource.ResolveAsync(
                Path.Combine(temporary.Path, "missing", AppConstants.DbFileBasename)));
        Assert.True(LockService.IsLockUnverifiable(error));
    }
}

internal sealed class StateDbTempDirectory : IDisposable
{
    public StateDbTempDirectory()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"codex-provider-sync-state-lock-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch
        {
            // Best-effort test cleanup on Windows antivirus/indexer races.
        }
    }
}
