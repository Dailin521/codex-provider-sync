using CodexProviderSync.Application;

namespace CodexProviderSync.Automation.Tests;

public sealed class AutomationCommandLineTests
{
    [Theory]
    [InlineData("unknown", "unknown_command")]
    [InlineData("sync", "option_required")]
    public void UnknownOrIncompleteCommands_AreControlledUsageFailures(string command, string errorCode)
    {
        AutomationParseResult parsed = AutomationCommandLine.Parse([command]);

        Assert.False(parsed.IsSuccess);
        Assert.Equal(AutomationExitCodes.ValidationOrUsage, parsed.Error!.ExitCode);
        Assert.Equal(errorCode, Assert.Single(parsed.Error.Errors).Code);
    }

    [Fact]
    public void RelativeAndCredentialPaths_AreRejectedBeforeAnyFileRead()
    {
        AutomationParseResult relative = AutomationCommandLine.Parse(
            ["status", "--codex-home", "../escape"]);
        string authPath = Path.Combine(Path.GetTempPath(), "auth.json");
        AutomationParseResult credential = AutomationCommandLine.Parse(
            ["sync", "--codex-home", Path.GetTempPath(), "--provider", "relay", "--apply", "--plan", authPath, "--plan-digest", new string('a', 64)]);

        Assert.Equal("path_escape", Assert.Single(relative.Error!.Errors).Code);
        // Missing files are rejected before their contents can be inspected.
        Assert.Contains(
            Assert.Single(credential.Error!.Errors).Code,
            new[] { "path_not_found", "credential_path_forbidden" });
    }

    [Fact]
    public void AllWriteInputs_AreNormalizedAndTyped()
    {
        using TemporaryDirectory temporary = new();
        string backup = Directory.CreateDirectory(Path.Combine(temporary.Path, "backup")).FullName;
        string sqliteHome = Directory.CreateDirectory(Path.Combine(temporary.Path, "sqlite")).FullName;
        string homeWithDots = Path.Combine(temporary.Path, "child", "..", "home");
        Directory.CreateDirectory(Path.GetFullPath(homeWithDots));
        AutomationParseResult change = AutomationCommandLine.Parse(
        [
            "switch", "--codex-home", homeWithDots, "--provider", "  relay  ",
            "--model-mode", "custom", "--model", "  model-x  ", "--keep", "3"
        ]);
        AutomationParseResult restore = AutomationCommandLine.Parse(
        [
            "plan", "--operation", "restore", "--codex-home", temporary.Path,
            "--backup", backup, "--sqlite-home", sqliteHome,
            "--no-config", "--allow-sqlite-home-relocation"
        ]);

        SwitchIntent switchIntent = Assert.IsType<SwitchIntent>(change.Invocation!.Intent);
        Assert.Equal(Path.GetFullPath(homeWithDots), switchIntent.CodexHome);
        Assert.Equal("relay", switchIntent.ProviderId);
        Assert.Equal("model-x", Assert.IsType<CustomModelSelection>(switchIntent.ModelSelection).Model);
        Assert.Equal(3, switchIntent.BackupRetentionCount);
        RestoreIntent restoreIntent = Assert.IsType<RestoreIntent>(restore.Invocation!.Intent);
        Assert.False(restoreIntent.RestoreConfig);
        Assert.True(restoreIntent.RestoreDatabase);
        Assert.True(restoreIntent.RestoreSessions);
        Assert.True(restoreIntent.AllowSqliteHomeRelocation);
    }

    [Theory]
    [InlineData("restore")]
    [InlineData("plan")]
    public void SqliteHomeRelocation_RequiresExplicitSqliteHomeAndNoConfig(string command)
    {
        using TemporaryDirectory temporary = new();
        string backup = Directory.CreateDirectory(Path.Combine(temporary.Path, "backup")).FullName;
        string sqliteHome = Directory.CreateDirectory(Path.Combine(temporary.Path, "sqlite")).FullName;

        List<string> missingSqlite = command == "plan"
            ? ["plan", "--operation", "restore", "--codex-home", temporary.Path, "--backup", backup, "--no-config", "--allow-sqlite-home-relocation"]
            : ["restore", "--codex-home", temporary.Path, "--backup", backup, "--no-config", "--allow-sqlite-home-relocation"];
        List<string> restoresConfig = command == "plan"
            ? ["plan", "--operation", "restore", "--codex-home", temporary.Path, "--backup", backup, "--sqlite-home", sqliteHome, "--allow-sqlite-home-relocation"]
            : ["restore", "--codex-home", temporary.Path, "--backup", backup, "--sqlite-home", sqliteHome, "--allow-sqlite-home-relocation"];

        AutomationParseResult withoutSqlite = AutomationCommandLine.Parse(missingSqlite);
        AutomationParseResult withConfigRestore = AutomationCommandLine.Parse(restoresConfig);

        Assert.Equal(AutomationExitCodes.ValidationOrUsage, withoutSqlite.Error!.ExitCode);
        Assert.Equal(
            "sqlite_home_relocation_requires_explicit_target_and_no_config",
            Assert.Single(withoutSqlite.Error.Errors).Code);
        Assert.Equal(
            "sqlite_home_relocation_requires_explicit_target_and_no_config",
            Assert.Single(withConfigRestore.Error!.Errors).Code);
    }

    [Fact]
    public void ApplyRequiresExactDigestAndPlanTogether()
    {
        using TemporaryDirectory temporary = new();
        string plan = Path.Combine(temporary.Path, "plan.json");
        File.WriteAllText(plan, "{}");

        AutomationParseResult missingDigest = AutomationCommandLine.Parse(
        [
            "sync", "--codex-home", temporary.Path, "--provider", "relay",
            "--apply", "--plan", plan
        ]);
        AutomationParseResult uppercaseDigest = AutomationCommandLine.Parse(
        [
            "sync", "--codex-home", temporary.Path, "--provider", "relay",
            "--apply", "--plan", plan, "--plan-digest", new string('A', 64)
        ]);

        Assert.Equal("plan_required", Assert.Single(missingDigest.Error!.Errors).Code);
        Assert.Equal("plan_digest_invalid", Assert.Single(uppercaseDigest.Error!.Errors).Code);
    }
}

internal sealed class TemporaryDirectory : IDisposable
{
    public TemporaryDirectory()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"codex-provider-sync-automation-{Guid.NewGuid():N}");
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
            // Test cleanup must not hide the assertion result.
        }
    }
}
