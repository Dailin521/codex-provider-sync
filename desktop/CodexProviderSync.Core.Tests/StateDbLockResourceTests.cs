using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using CodexProviderSync.Core;

namespace CodexProviderSync.Core.Tests;

public sealed class StateDbLockResourceTests
{
    [WindowsStateDbAliasFact]
    public async Task ResolveAsync_WindowsCaseAliases_UseOneResourceAndContend()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "CaseSensitiveLookingSqliteHome");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");

        StateDbLockResource canonical = await StateDbLockResource.ResolveAsync(stateDbPath);
        StateDbLockResource alias = await StateDbLockResource.ResolveAsync(stateDbPath.ToUpperInvariant());

        AssertSameResource(canonical, alias);
        await AssertAliasesContendAsync(canonical, alias);
    }

    [WindowsStateDbAliasFact]
    public async Task ResolveAsync_WindowsDirectorySymlink_UsesOneResourceAndContends()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "physical-sqlite-home");
        string aliasHome = Path.Combine(temporary.Path, "sqlite-home-alias");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");
        WindowsDirectoryAlias.CreateJunction(aliasHome, sqliteHome);

        StateDbLockResource canonical = await StateDbLockResource.ResolveAsync(stateDbPath);
        StateDbLockResource alias = await StateDbLockResource.ResolveAsync(
            Path.Combine(aliasHome, AppConstants.DbFileBasename));

        AssertSameResource(canonical, alias);
        await AssertAliasesContendAsync(canonical, alias);
    }

    [WindowsStateDbAliasFact]
    public async Task ResolveAsync_WindowsShortAndLongDirectoryPaths_UseOneResourceAndContend()
    {
        using StateDbTempDirectory temporary = new();
        string sqliteHome = Path.Combine(temporary.Path, "Long SQLite Home For Physical Alias");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");
        string shortHome = WindowsDirectoryAlias.GetShortPath(sqliteHome);
        if (string.Equals(shortHome, sqliteHome, StringComparison.OrdinalIgnoreCase))
        {
            throw Xunit.Sdk.SkipException.ForSkip(
                "The temporary volume did not provide an actual Windows 8.3 directory alias.");
        }

        StateDbLockResource canonical = await StateDbLockResource.ResolveAsync(stateDbPath);
        StateDbLockResource alias = await StateDbLockResource.ResolveAsync(
            Path.Combine(shortHome, AppConstants.DbFileBasename));

        AssertSameResource(canonical, alias);
        await AssertAliasesContendAsync(canonical, alias);
    }

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

    private static void AssertSameResource(
        StateDbLockResource expected,
        StateDbLockResource actual)
    {
        Assert.Equal(expected.Identity, actual.Identity);
        Assert.Equal(expected.ResourceKey, actual.ResourceKey);
        Assert.Equal(expected.RealDbParent, actual.RealDbParent);
        Assert.Equal(expected.StateDbPath, actual.StateDbPath);
        Assert.Equal(expected.LockPath, actual.LockPath);
    }

    private static async Task AssertAliasesContendAsync(
        StateDbLockResource owner,
        StateDbLockResource contender)
    {
        LockService service = new();
        await using LockHandle held = await service.AcquireStateDbLockAsync(owner, "alias-owner");
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.AcquireStateDbLockAsync(contender, "alias-contender"));
        Assert.True(LockService.IsOperationBusy(error));
        Assert.Equal("state-db", error.Data["codex-provider-sync/lock-scope"]);
        Assert.Equal(owner.ResourceKey, error.Data["codex-provider-sync/resource-key"]);
    }

}

public sealed class WindowsStateDbAliasFactAttribute : FactAttribute
{
    public WindowsStateDbAliasFactAttribute()
    {
        if (!OperatingSystem.IsWindows())
        {
            Skip = "Windows path-alias semantics are not applicable on this platform.";
        }
    }
}

internal static class WindowsDirectoryAlias
{
    public static string GetShortPath(string value)
    {
        StringBuilder buffer = new(32768);
        uint length = GetShortPathNameW(value, buffer, (uint)buffer.Capacity);
        if (length == 0 || length >= buffer.Capacity)
        {
            throw new InvalidOperationException(
                $"GetShortPathNameW could not resolve an 8.3 alias (Win32 {Marshal.GetLastPInvokeError()}).");
        }
        return buffer.ToString();
    }

    public static void CreateJunction(string aliasPath, string targetPath)
    {
        ProcessStartInfo startInfo = new(Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.System),
            "cmd.exe"))
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        startInfo.ArgumentList.Add("/d");
        startInfo.ArgumentList.Add("/c");
        startInfo.ArgumentList.Add("mklink");
        startInfo.ArgumentList.Add("/J");
        startInfo.ArgumentList.Add(aliasPath);
        startInfo.ArgumentList.Add(targetPath);
        using Process process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the Windows junction helper.");
        string stdout = process.StandardOutput.ReadToEnd();
        string stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0 || !Directory.Exists(aliasPath))
        {
            throw new InvalidOperationException(
                $"Could not create the test junction (exit {process.ExitCode}). {stdout} {stderr}".Trim());
        }
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetShortPathNameW(
        string longPath,
        StringBuilder shortPath,
        uint shortPathLength);
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
