using System.Diagnostics;
using System.Text.Json;
using CodexProviderSync.Core;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core.Tests;

public sealed class CrossRuntimeStateDbLockTests
{
    [Fact]
    public async Task DotNetOwner_BlocksARealNodeContenderWithTheSameResourceKey()
    {
        using StateDbTempDirectory temporary = new();
        string stateDbPath = await CreateStateDbFixtureAsync(temporary.Path);
        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(stateDbPath);
        await using LockHandle held = await new LockService().AcquireStateDbLockAsync(resource, "dotnet-winner");

        ProcessResult result = await RunNodeAsync($$"""
            import { acquireStateDbLock } from {{JsonSerializer.Serialize(ModuleUrl())}};
            try {
              const held = await acquireStateDbLock({{JsonSerializer.Serialize(stateDbPath)}}, "node-contender");
              await held.release();
              console.log(JSON.stringify({ code: "ACQUIRED", resourceKey: held.resource.resourceKey }));
              process.exit(0);
            } catch (error) {
              console.log(JSON.stringify({ code: error?.code, busyScope: error?.details?.busyScope }));
              process.exit(5);
            }
            """);

        Assert.Equal(5, result.ExitCode);
        using JsonDocument payload = JsonDocument.Parse(result.StdOut.Trim());
        Assert.Equal("OPERATION_BUSY", payload.RootElement.GetProperty("code").GetString());
        Assert.Equal("state-db", payload.RootElement.GetProperty("busyScope").GetString());
    }

    [Fact]
    public async Task NodeOwner_BlocksADotNetContenderWithTheSameResourceKey()
    {
        using StateDbTempDirectory temporary = new();
        string stateDbPath = await CreateStateDbFixtureAsync(temporary.Path);
        StateDbLockResource resource = await StateDbLockResource.ResolveAsync(stateDbPath);
        string script = $$"""
            import { acquireStateDbLock } from {{JsonSerializer.Serialize(ModuleUrl())}};
            const held = await acquireStateDbLock({{JsonSerializer.Serialize(stateDbPath)}}, "node-winner");
            console.log(JSON.stringify({ ready: true, resourceKey: held.resource.resourceKey }));
            await new Promise((resolve) => process.stdin.once("data", resolve));
            await held.release();
            """;
        using Process child = StartNode(script);
        string readyLine = await ReadLineWithTimeoutAsync(child.StandardOutput, TimeSpan.FromSeconds(15));
        using JsonDocument ready = JsonDocument.Parse(readyLine);
        Assert.True(ready.RootElement.GetProperty("ready").GetBoolean());
        Assert.Equal(resource.ResourceKey, ready.RootElement.GetProperty("resourceKey").GetString());

        try
        {
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new LockService().AcquireStateDbLockAsync(resource, "dotnet-contender"));
            Assert.True(LockService.IsOperationBusy(error));
            Assert.Equal("state-db", error.Data["codex-provider-sync/lock-scope"]);
        }
        finally
        {
            await child.StandardInput.WriteLineAsync("release");
            child.StandardInput.Close();
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await child.WaitForExitAsync(timeout.Token);
            Assert.Equal(0, child.ExitCode);
        }
    }

    [Fact]
    public async Task RealNodeOwner_MakesDotNetStatusReturnItsLastCompleteSnapshot()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        await fixture.WriteStateDbAsync([("thread-node-status", "relay", false)]);
        CodexSyncService service = new();
        StatusSnapshot baseline = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal(1, baseline.SqliteCounts!.Sessions["relay"]);

        string script = $$"""
            import { acquireStateDbLock } from {{JsonSerializer.Serialize(ModuleUrl())}};
            const held = await acquireStateDbLock({{JsonSerializer.Serialize(fixture.StateDbPath())}}, "node-status-writer");
            console.log(JSON.stringify({ ready: true, resourceKey: held.resource.resourceKey }));
            await new Promise((resolve) => process.stdin.once("data", resolve));
            await held.release();
            """;
        using Process child = StartNode(script);
        string readyLine = await ReadLineWithTimeoutAsync(child.StandardOutput, TimeSpan.FromSeconds(15));
        using JsonDocument ready = JsonDocument.Parse(readyLine);
        Assert.True(ready.RootElement.GetProperty("ready").GetBoolean());

        try
        {
            await using SqliteConnection connection = fixture.OpenSqliteConnection();
            await connection.OpenAsync();
            SqliteCommand update = connection.CreateCommand();
            update.CommandText = "UPDATE threads SET model_provider = 'external' WHERE id = 'thread-node-status'";
            Assert.Equal(1, await update.ExecuteNonQueryAsync());

            StatusSnapshot blocked = await new CodexSyncService().GetStatusAsync(fixture.CodexHome);
            Assert.Equal(1, blocked.SqliteCounts!.Sessions["relay"]);
            Assert.False(blocked.SqliteCounts.Sessions.ContainsKey("external"));
            Assert.Equal("state-db", blocked.OperationInProgress!.BusyScope);
            Assert.Equal("node", blocked.OperationInProgress.Runtime);
            Assert.Equal("node-status-writer", blocked.OperationInProgress.Operation);
        }
        finally
        {
            await child.StandardInput.WriteLineAsync("release");
            child.StandardInput.Close();
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await child.WaitForExitAsync(timeout.Token);
            Assert.Equal(0, child.ExitCode);
        }

        StatusSnapshot refreshed = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal(1, refreshed.SqliteCounts!.Sessions["external"]);
    }

    [Fact]
    public async Task RealNodeHomeOwner_MakesDotNetStatusReturnItsLastCompleteSnapshot()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"apigather\"");
        await fixture.WriteStateDbAsync([("thread-node-home-status", "apigather", false)]);
        CodexSyncService service = new();
        StatusSnapshot baseline = await service.GetStatusAsync(fixture.CodexHome);
        Assert.Equal("apigather", baseline.CurrentProvider.Provider);

        string script = $$"""
            import { acquireLock } from {{JsonSerializer.Serialize(LockingModuleUrl())}};
            const release = await acquireLock({{JsonSerializer.Serialize(fixture.CodexHome)}}, "node-home-status-writer");
            console.log(JSON.stringify({ ready: true }));
            await new Promise((resolve) => process.stdin.once("data", resolve));
            await release();
            """;
        using Process child = StartNode(script);
        string readyLine = await ReadLineWithTimeoutAsync(child.StandardOutput, TimeSpan.FromSeconds(15));
        using JsonDocument ready = JsonDocument.Parse(readyLine);
        Assert.True(ready.RootElement.GetProperty("ready").GetBoolean());

        try
        {
            await fixture.WriteConfigAsync("model_provider = \"openai\"");
            StatusSnapshot blocked = await new CodexSyncService().GetStatusAsync(fixture.CodexHome);
            Assert.Equal("apigather", blocked.CurrentProvider.Provider);
            Assert.Equal("codex-home", blocked.OperationInProgress!.BusyScope);
            Assert.Equal("node", blocked.OperationInProgress.Runtime);
            Assert.Equal("node-home-status-writer", blocked.OperationInProgress.Operation);
        }
        finally
        {
            await child.StandardInput.WriteLineAsync("release");
            child.StandardInput.Close();
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await child.WaitForExitAsync(timeout.Token);
            Assert.Equal(0, child.ExitCode);
        }

        Assert.Equal("openai", (await service.GetStatusAsync(fixture.CodexHome)).CurrentProvider.Provider);
    }

    [Fact]
    public async Task DifferentHomeStatus_SharingNodeLockedStateDb_UsesItsOwnCachedSnapshot()
    {
        TestCodexHomeFixture first = await TestCodexHomeFixture.CreateAsync();
        TestCodexHomeFixture second = await TestCodexHomeFixture.CreateAsync();
        await first.WriteConfigAsync("model_provider = \"openai\"");
        await second.WriteConfigAsync("model_provider = \"openai\"");
        string sharedSqliteHome = Path.Combine(first.Root, "shared-status-sqlite");
        string sharedStateDb = Path.Combine(sharedSqliteHome, AppConstants.DbFileBasename);
        await first.WriteStateDbAtAsync(
            sharedStateDb,
            [("thread-shared-status", "relay", false)],
            model: null);
        CodexSyncService service = new();
        StatusSnapshot baseline = await service.GetStatusAsync(second.CodexHome, sharedSqliteHome);
        Assert.Equal(1, baseline.SqliteCounts!.Sessions["relay"]);

        string script = $$"""
            import { acquireStateDbLock } from {{JsonSerializer.Serialize(ModuleUrl())}};
            const held = await acquireStateDbLock({{JsonSerializer.Serialize(sharedStateDb)}}, "node-shared-db-writer");
            console.log(JSON.stringify({ ready: true, resourceKey: held.resource.resourceKey }));
            await new Promise((resolve) => process.stdin.once("data", resolve));
            await held.release();
            """;
        using Process child = StartNode(script);
        string readyLine = await ReadLineWithTimeoutAsync(child.StandardOutput, TimeSpan.FromSeconds(15));
        using JsonDocument ready = JsonDocument.Parse(readyLine);
        Assert.True(ready.RootElement.GetProperty("ready").GetBoolean());

        try
        {
            await using SqliteConnection connection = new($"Data Source={sharedStateDb};Pooling=False");
            await connection.OpenAsync();
            SqliteCommand update = connection.CreateCommand();
            update.CommandText = "UPDATE threads SET model_provider = 'external' WHERE id = 'thread-shared-status'";
            Assert.Equal(1, await update.ExecuteNonQueryAsync());

            StatusSnapshot blocked = await new CodexSyncService().GetStatusAsync(
                second.CodexHome,
                sharedSqliteHome);
            Assert.Equal(1, blocked.SqliteCounts!.Sessions["relay"]);
            Assert.False(blocked.SqliteCounts.Sessions.ContainsKey("external"));
            Assert.Equal("state-db", blocked.OperationInProgress!.BusyScope);
            Assert.Equal("node-shared-db-writer", blocked.OperationInProgress.Operation);
        }
        finally
        {
            await child.StandardInput.WriteLineAsync("release");
            child.StandardInput.Close();
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(15));
            await child.WaitForExitAsync(timeout.Token);
            Assert.Equal(0, child.ExitCode);
        }

        StatusSnapshot refreshed = await service.GetStatusAsync(second.CodexHome, sharedSqliteHome);
        Assert.Equal(1, refreshed.SqliteCounts!.Sessions["external"]);
    }

    private static async Task<string> CreateStateDbFixtureAsync(string root)
    {
        string sqliteHome = Path.Combine(root, "sqlite");
        Directory.CreateDirectory(sqliteHome);
        string stateDbPath = Path.Combine(sqliteHome, AppConstants.DbFileBasename);
        await File.WriteAllTextAsync(stateDbPath, "fixture");
        return stateDbPath;
    }

    private static string ModuleUrl() => new Uri(
        Path.Combine(FindRepositoryRoot(), "src", "state-db-lock.js")).AbsoluteUri;

    private static string LockingModuleUrl() => new Uri(
        Path.Combine(FindRepositoryRoot(), "src", "locking.js")).AbsoluteUri;

    private static string FindRepositoryRoot()
    {
        foreach (string start in new[] { Environment.CurrentDirectory, AppContext.BaseDirectory })
        {
            DirectoryInfo? current = new(Path.GetFullPath(start));
            while (current is not null)
            {
                if (File.Exists(Path.Combine(current.FullName, "src", "state-db-lock.js")))
                {
                    return current.FullName;
                }
                current = current.Parent;
            }
        }
        throw new DirectoryNotFoundException("Cannot locate the repository root for the Node lock parity test.");
    }

    private static Process StartNode(string script)
    {
        ProcessStartInfo startInfo = new("node")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = FindRepositoryRoot()
        };
        startInfo.ArgumentList.Add("--input-type=module");
        startInfo.ArgumentList.Add("-e");
        startInfo.ArgumentList.Add(script);
        return Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the Node lock parity process.");
    }

    private static async Task<ProcessResult> RunNodeAsync(string script)
    {
        using Process process = StartNode(script);
        Task<string> stdout = process.StandardOutput.ReadToEndAsync();
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
        await process.WaitForExitAsync(timeout.Token);
        return new ProcessResult(process.ExitCode, await stdout, await stderr);
    }

    private static async Task<string> ReadLineWithTimeoutAsync(StreamReader reader, TimeSpan timeout)
    {
        using CancellationTokenSource cancellation = new(timeout);
        return await reader.ReadLineAsync(cancellation.Token)
            ?? throw new InvalidOperationException("Node lock parity process exited before publishing readiness.");
    }

    private sealed record ProcessResult(int ExitCode, string StdOut, string StdErr);
}
