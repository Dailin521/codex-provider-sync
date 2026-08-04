using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using CodexProviderSync.Core;

namespace CodexProviderSync.Automation.Tests;

public sealed class AutomationProcessAndSchemaTests
{
    [Fact]
    public void Schema_IsEmbeddedCopiedAndDeclaresTheStableProtocolSurface()
    {
        Assembly assembly = typeof(Program).Assembly;
        string resourceName = "CodexProviderSync.Automation.automation-protocol-v0.4.schema.json";
        Assert.Contains(resourceName, assembly.GetManifestResourceNames());
        using Stream stream = assembly.GetManifestResourceStream(resourceName)!;
        using JsonDocument schema = JsonDocument.Parse(stream);
        JsonElement root = schema.RootElement;

        Assert.Equal("https://json-schema.org/draft/2020-12/schema", root.GetProperty("$schema").GetString());
        JsonElement definitions = root.GetProperty("$defs");
        JsonElement exitCodes = definitions.GetProperty("response").GetProperty("properties").GetProperty("exitCode").GetProperty("enum");
        Assert.Equal(new[] { 0, 2, 3, 4, 5, 6, 7, 10 }, exitCodes.EnumerateArray().Select(static value => value.GetInt32()));
        Assert.Equal(
            "^[0-9a-f]{64}$",
            definitions.GetProperty("operationPlan").GetProperty("properties").GetProperty("digest").GetProperty("pattern").GetString());
        string copied = Path.Combine(Path.GetDirectoryName(assembly.Location)!, "automation-protocol-v0.4.schema.json");
        Assert.True(File.Exists(copied));
    }

    [Fact]
    public async Task Process_EmitsExactlyOneJsonDocumentAndUsesStderrForDiagnostics()
    {
        ProcessResult described = await RunProcessAsync("describe");
        ProcessResult unknown = await RunProcessAsync("unknown-command");

        Assert.Equal(0, described.ExitCode);
        Assert.Single(NonEmptyLines(described.StdOut));
        using JsonDocument description = JsonDocument.Parse(described.StdOut);
        Assert.Equal("0.4", description.RootElement.GetProperty("protocolVersion").GetString());
        Assert.Equal("success", description.RootElement.GetProperty("result").GetString());
        Assert.True(string.IsNullOrWhiteSpace(described.StdErr));

        Assert.Equal(AutomationExitCodes.ValidationOrUsage, unknown.ExitCode);
        Assert.Single(NonEmptyLines(unknown.StdOut));
        using JsonDocument failure = JsonDocument.Parse(unknown.StdOut);
        Assert.Equal("unknown_command", failure.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("Unknown command", unknown.StdErr, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Process_StatusUsesOnlyTheExplicitFixtureHome()
    {
        using TemporaryDirectory temporary = new();
        await File.WriteAllTextAsync(
            Path.Combine(temporary.Path, "config.toml"),
            "model_provider = \"openai\"\n");

        ProcessResult result = await RunProcessAsync("status", "--codex-home", temporary.Path);

        Assert.Equal(0, result.ExitCode);
        using JsonDocument json = JsonDocument.Parse(result.StdOut);
        Assert.Equal(
            Path.GetFullPath(temporary.Path),
            json.RootElement.GetProperty("data").GetProperty("codexHome").GetString());
        Assert.False(File.Exists(Path.Combine(temporary.Path, "auth.json")));
    }

    [Fact]
    public async Task Process_WritePlanUsesTheProductionCoreAdapterWithoutMutation()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        Directory.CreateDirectory(Path.Combine(temporary.Path, "sessions"));
        Directory.CreateDirectory(Path.Combine(temporary.Path, "archived_sessions"));
        await File.WriteAllTextAsync(
            Path.Combine(temporary.Path, "config.toml"),
            "model_provider = \"openai\"\n");

        ProcessResult result = await RunProcessAsync(
            "plan",
            "--operation",
            "sync",
            "--codex-home",
            temporary.Path,
            "--provider",
            "relay",
            "--ledger-root",
            ledger);

        Assert.Equal(AutomationExitCodes.Success, result.ExitCode);
        using JsonDocument json = JsonDocument.Parse(result.StdOut);
        Assert.Equal("readyToApply", json.RootElement.GetProperty("lifecycle").GetString());
        Assert.Equal(64, json.RootElement.GetProperty("data").GetProperty("digest").GetString()!.Length);
        Assert.True(Directory.Exists(ledger));
        Assert.False(Directory.Exists(Path.Combine(
            temporary.Path,
            "backups_state",
            "provider-sync")));
    }

    [Fact]
    public async Task Process_ExplicitApplyExecutesExactPlanOnceAcrossProcessBoundaries()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string sessions = Path.Combine(temporary.Path, "sessions");
        Directory.CreateDirectory(sessions);
        Directory.CreateDirectory(Path.Combine(temporary.Path, "archived_sessions"));
        await File.WriteAllTextAsync(
            Path.Combine(temporary.Path, "config.toml"),
            "model_provider = \"openai\"\n");
        string rolloutPath = Path.Combine(sessions, "rollout-process.jsonl");
        await File.WriteAllTextAsync(
            rolloutPath,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-process\",\"cwd\":\"C:\\\\fixture\",\"model_provider\":\"relay\"}}\n");

        ProcessResult planned = await RunProcessAsync(
            "plan", "--operation", "sync",
            "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger);
        Assert.Equal(AutomationExitCodes.Success, planned.ExitCode);
        using JsonDocument planResponse = JsonDocument.Parse(planned.StdOut);
        JsonElement plan = planResponse.RootElement.GetProperty("data");
        string digest = plan.GetProperty("digest").GetString()!;
        string planPath = Path.Combine(temporary.Path, "plan.json");
        await File.WriteAllTextAsync(planPath, plan.GetRawText());

        ProcessResult applied = await RunProcessAsync(
            "sync", "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger,
            "--apply", "--plan", planPath,
            "--plan-digest", digest);
        ProcessResult duplicate = await RunProcessAsync(
            "sync", "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger,
            "--apply", "--plan", planPath,
            "--plan-digest", digest);

        Assert.Equal(AutomationExitCodes.Success, applied.ExitCode);
        Assert.Contains("\"model_provider\":\"openai\"", await File.ReadAllTextAsync(rolloutPath));
        Assert.Equal(AutomationExitCodes.InvalidPlan, duplicate.ExitCode);
        using JsonDocument duplicateResponse = JsonDocument.Parse(duplicate.StdOut);
        Assert.Equal(
            "plan_already_used",
            duplicateResponse.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Process_DriftedPlanReturnsExitThreeWithoutCreatingABackup()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string sessions = Path.Combine(temporary.Path, "sessions");
        Directory.CreateDirectory(sessions);
        Directory.CreateDirectory(Path.Combine(temporary.Path, "archived_sessions"));
        await File.WriteAllTextAsync(
            Path.Combine(temporary.Path, "config.toml"),
            "model_provider = \"openai\"\n");
        string rolloutPath = Path.Combine(sessions, "rollout-drift.jsonl");
        await File.WriteAllTextAsync(
            rolloutPath,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-drift\",\"cwd\":\"C:\\\\fixture\",\"model_provider\":\"relay\"}}\n");
        ProcessResult planned = await RunProcessAsync(
            "plan", "--operation", "sync",
            "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger);
        using JsonDocument planResponse = JsonDocument.Parse(planned.StdOut);
        JsonElement plan = planResponse.RootElement.GetProperty("data");
        string digest = plan.GetProperty("digest").GetString()!;
        string planPath = Path.Combine(temporary.Path, "plan.json");
        await File.WriteAllTextAsync(planPath, plan.GetRawText());
        await File.AppendAllTextAsync(
            rolloutPath,
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\"}}\n");
        string drifted = await File.ReadAllTextAsync(rolloutPath);

        ProcessResult applied = await RunProcessAsync(
            "sync", "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger,
            "--apply", "--plan", planPath,
            "--plan-digest", digest);

        Assert.Equal(AutomationExitCodes.InvalidPlan, applied.ExitCode);
        using JsonDocument response = JsonDocument.Parse(applied.StdOut);
        Assert.Equal(
            "plan_stale",
            response.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(drifted, await File.ReadAllTextAsync(rolloutPath));
        Assert.False(Directory.Exists(Path.Combine(
            temporary.Path,
            "backups_state",
            "provider-sync")));
    }

    [Fact]
    public async Task Process_CoreLockContentionReturnsExitFourWithoutMutation()
    {
        using TemporaryDirectory temporary = new();
        string ledger = Path.Combine(temporary.Path, "ledger");
        string sessions = Path.Combine(temporary.Path, "sessions");
        Directory.CreateDirectory(sessions);
        Directory.CreateDirectory(Path.Combine(temporary.Path, "archived_sessions"));
        await File.WriteAllTextAsync(
            Path.Combine(temporary.Path, "config.toml"),
            "model_provider = \"openai\"\n");
        string rolloutPath = Path.Combine(sessions, "rollout-busy.jsonl");
        await File.WriteAllTextAsync(
            rolloutPath,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-busy\",\"cwd\":\"C:\\\\fixture\",\"model_provider\":\"relay\"}}\n");
        ProcessResult planned = await RunProcessAsync(
            "plan", "--operation", "sync",
            "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger);
        using JsonDocument planResponse = JsonDocument.Parse(planned.StdOut);
        JsonElement plan = planResponse.RootElement.GetProperty("data");
        string digest = plan.GetProperty("digest").GetString()!;
        string planPath = Path.Combine(temporary.Path, "plan.json");
        await File.WriteAllTextAsync(planPath, plan.GetRawText());
        string before = await File.ReadAllTextAsync(rolloutPath);

        await using LockHandle held = await new LockService().AcquireLockAsync(
            temporary.Path,
            "automation-process-test");
        ProcessResult applied = await RunProcessAsync(
            "sync", "--codex-home", temporary.Path,
            "--provider", "openai",
            "--ledger-root", ledger,
            "--apply", "--plan", planPath,
            "--plan-digest", digest);

        Assert.Equal(AutomationExitCodes.Busy, applied.ExitCode);
        using JsonDocument response = JsonDocument.Parse(applied.StdOut);
        Assert.Equal(
            "target_busy",
            response.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(before, await File.ReadAllTextAsync(rolloutPath));
        Assert.False(Directory.Exists(Path.Combine(
            temporary.Path,
            "backups_state",
            "provider-sync")));
    }

    private static async Task<ProcessResult> RunProcessAsync(params string[] args)
    {
        string assemblyPath = typeof(Program).Assembly.Location;
        ProcessStartInfo start = new("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add(assemblyPath);
        foreach (string argument in args)
        {
            start.ArgumentList.Add(argument);
        }

        using Process process = Process.Start(start)!;
        string stdout = await process.StandardOutput.ReadToEndAsync();
        string stderr = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return new ProcessResult(process.ExitCode, stdout, stderr);
    }

    private static string[] NonEmptyLines(string value)
    {
        return value.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private sealed record ProcessResult(int ExitCode, string StdOut, string StdErr);
}
