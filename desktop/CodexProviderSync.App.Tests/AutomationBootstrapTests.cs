using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CodexProviderSync.App.Automation;
using CodexProviderSync.Core;
using System.Windows.Forms;

namespace CodexProviderSync.App.Tests;

public sealed class AutomationBootstrapTests
{
    [Fact]
    public void FolderPickerFactory_ProducesARealConfiguredWinFormsDialog()
    {
        string initial = Path.GetTempPath();
        FolderPickerRequest request = new(
            "isolated real folder picker",
            initial,
            AllowCreate: true);

        using FolderBrowserDialog dialog = AppFolderPickerDialog.Create(request);

        Assert.Equal(request.Description, dialog.Description);
        Assert.True(dialog.UseDescriptionForTitle);
        Assert.Equal(initial, dialog.InitialDirectory);
        Assert.True(dialog.ShowNewFolderButton);
    }

    [Fact]
    public void IsolatedFolderPicker_RejectsAnExternalInitialPathBeforeShowingUi()
    {
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        IsolatedAppPlatformBoundary boundary = new(paths);
        string outside = Path.Combine(
            Path.GetDirectoryName(fixture.Root)!,
            $"outside-picker-{Guid.NewGuid():N}");

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() =>
            boundary.PickFolder(
                owner: null!,
                new FolderPickerRequest("must stay isolated", outside)));

        Assert.Contains("outside", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task IsolatedUpdateEntry_IsEnabledAndReturnsDeterministicNoNetworkState()
    {
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        IsolatedAppPlatformBoundary boundary = new(paths);
        RejectingNetworkHandler handler = new();
        UpdateService updateService = new(new HttpClient(handler));

        UpdateCheckResult result = await boundary.CheckForUpdateAsync(
            updateService,
            new Version(0, 4, 0));

        Assert.True(boundary.UpdatesEnabled);
        Assert.False(result.IsUpdateAvailable);
        Assert.Equal(new Version(0, 4, 0), result.CurrentVersion);
        Assert.Equal(0, handler.RequestCount);
    }

    [Fact]
    public void ValidDescriptor_IsClaimedOnce_AndEveryPathStaysInsideTheSentinelRoot()
    {
        using AutomationRoot fixture = new();
        string descriptor = fixture.WriteDescriptor();

        AutomationBootstrap bootstrap = AutomationBootstrap.ParseAndClaim(
            [AutomationBootstrap.Argument, descriptor]);
        IAppPathProvider paths = new IsolatedAppPathProvider(bootstrap.IsolationRoot!);

        Assert.True(bootstrap.Enabled);
        Assert.True(File.Exists(descriptor + ".claimed"));
        Assert.All(new[]
        {
            paths.SettingsPath,
            paths.LogDirectory,
            paths.SingletonDirectory,
            paths.DefaultCodexHome,
            paths.RequiredSqliteHomeOverride!,
            paths.UpdateDownloadDirectory,
            paths.UpdaterRoot,
            paths.StartupErrorPath,
            paths.AutomationTracePath!
        }, path => Assert.True(paths.Contains(path), path));

        InvalidOperationException replay = Assert.Throws<InvalidOperationException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, descriptor]));
        Assert.Contains("replay", replay.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DescriptorOutsideDeclaredRoot_IsRejectedWithoutAClaim()
    {
        using AutomationRoot fixture = new();
        string other = Path.Combine(Path.GetDirectoryName(fixture.Root)!, $"outside-{Guid.NewGuid():N}.json");
        try
        {
            fixture.WriteDescriptor(other);
            Assert.Throws<InvalidDataException>(() =>
                AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, other]));
            Assert.False(File.Exists(other + ".claimed"));
        }
        finally
        {
            File.Delete(other);
        }
    }

    [Fact]
    public void MissingOrWrongSentinel_RandomPipeTokenAndExactArguments_AreFailClosed()
    {
        using AutomationRoot missingSentinel = new(createSentinel: false);
        string missingDescriptor = missingSentinel.WriteDescriptor();
        Assert.ThrowsAny<Exception>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, missingDescriptor]));

        using AutomationRoot invalidPipe = new();
        string invalidPipeDescriptor = invalidPipe.WriteDescriptor(pipeName: "CodexProviderSync.Automation.fixed");
        Assert.Throws<InvalidDataException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, invalidPipeDescriptor]));

        using AutomationRoot invalidToken = new();
        string invalidTokenDescriptor = invalidToken.WriteDescriptor(token: "abcd");
        Assert.Throws<InvalidDataException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, invalidTokenDescriptor]));

        using AutomationRoot lowEntropyToken = new();
        string lowEntropyDescriptor = lowEntropyToken.WriteDescriptor(token: new string('a', 64));
        Assert.Throws<InvalidDataException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, lowEntropyDescriptor]));

        Assert.Throws<InvalidOperationException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument]));
        Assert.Throws<InvalidOperationException>(() =>
            AutomationBootstrap.ParseAndClaim([AutomationBootstrap.Argument, "x", "--extra"]));
        Assert.Throws<InvalidOperationException>(() =>
            AutomationBootstrap.ParseAndClaim(["--gui-automation-descripto", "x"]));
    }

    [Fact]
    public void NormalArguments_DoNotEnableOrClaimAutomation()
    {
        AutomationBootstrap bootstrap = AutomationBootstrap.ParseAndClaim(["--apply-update"]);
        Assert.False(bootstrap.Enabled);
    }

    [Fact]
    public void IsolatedSingleInstanceGuard_UsesOnlyInjectedPathAndKeepsAProcessLease()
    {
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        AppInstanceGuard guard = new(paths);

        using AppInstanceAcquisition first = guard.Acquire();
        using AppInstanceAcquisition second = guard.Acquire();
        Assert.True(first.IsOwner);
        Assert.False(second.IsOwner);
        Assert.True(paths.Contains(paths.SingletonDirectory));

        first.Dispose();
        using AppInstanceAcquisition third = guard.Acquire();
        Assert.True(third.IsOwner);
    }

    [Fact]
    public async Task AutomationSettings_AreSanitizedBeforeControllerInitialization()
    {
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        SettingsService settings = new(paths.SettingsPath);
        string outside = Path.Combine(Path.GetDirectoryName(fixture.Root)!, "real-user-data");
        settings.Save(new AppSettings
        {
            LastCodexHome = outside,
            RecentCodexHomes = [outside],
            SqliteHomeOverrides = new Dictionary<string, string> { [outside] = outside },
            LastBackupDirectory = outside,
            ManualProviders = ["kept-provider"]
        });

        AutomationIsolation.PrepareSettings(settings, paths);

        AppSettings sanitized = await settings.LoadAsync();
        Assert.Equal(paths.DefaultCodexHome, sanitized.LastCodexHome);
        Assert.All(sanitized.RecentCodexHomes, path => Assert.True(paths.Contains(path)));
        Assert.Equal(
            paths.RequiredSqliteHomeOverride,
            sanitized.SqliteHomeOverrides[paths.DefaultCodexHome]);
        Assert.Null(sanitized.LastBackupDirectory);
        Assert.Equal(["kept-provider"], sanitized.ManualProviders);
        Assert.True(Directory.Exists(paths.DefaultCodexHome));
        Assert.True(Directory.Exists(paths.RequiredSqliteHomeOverride));
    }

    [Fact]
    public void ExternalConfigAndEnvironmentSqliteHomes_CannotEscapeTheAutomationRoot()
    {
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        SettingsService settings = new(paths.SettingsPath);
        AutomationIsolation.PrepareSettings(settings, paths);
        using MainForm form = new(
            new ExecutionLogService(paths.LogDirectory),
            settings,
            paths: paths,
            platformBoundary: new IsolatedAppPlatformBoundary(paths));

        string outsideRoot = Path.Combine(
            Path.GetDirectoryName(fixture.Root)!,
            $"real-sqlite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outsideRoot);
        string outsideDb = Path.Combine(outsideRoot, "state_5.sqlite");
        File.WriteAllBytes(outsideDb, RandomNumberGenerator.GetBytes(256));
        string before = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(outsideDb)));
        try
        {
            (string codexHome, string? sqliteHome) = form.CaptureStorageSelection();
            Assert.Equal(paths.DefaultCodexHome, codexHome);
            Assert.Equal(paths.RequiredSqliteHomeOverride, sqliteHome);

            string configText = $"model_provider = \"openai\"{Environment.NewLine}sqlite_home = \"{outsideRoot.Replace("\\", "/")}\"";
            CodexStorageLayout resolved = new CodexStorageLayoutService().Resolve(
                codexHome,
                sqliteHome,
                configText,
                new Dictionary<string, string?>
                {
                    ["CODEX_SQLITE_HOME"] = outsideRoot
                });
            Assert.Equal(paths.RequiredSqliteHomeOverride, resolved.SqliteHome);
            Assert.Equal("gui", resolved.SqliteHomeSource);

            StatusSnapshot safe = CreateStatus(paths);
            form.ValidateAutomationStatusSnapshot(safe);
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, codexHome: outsideRoot)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, sqliteHome: outsideRoot)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, stateDbPath: outsideDb)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, backupRoot: outsideRoot)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, checkedStateDbPath: outsideDb)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, lockedRolloutPath: outsideDb)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, unreadableRolloutPath: outsideDb)));
            Assert.Throws<InvalidOperationException>(() =>
                form.ValidateAutomationStatusSnapshot(CreateStatus(paths, pendingPath: outsideRoot)));
            Assert.Throws<InvalidOperationException>(() =>
                form.EnsureAutomationMutationBoundary(codexHome, sqliteHome, outsideRoot));

            string after = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(outsideDb)));
            Assert.Equal(before, after);
        }
        finally
        {
            Directory.Delete(outsideRoot, recursive: true);
        }
    }

    private static StatusSnapshot CreateStatus(
        IAppPathProvider paths,
        string? codexHome = null,
        string? sqliteHome = null,
        string? stateDbPath = null,
        string? backupRoot = null,
        string? checkedStateDbPath = null,
        string? lockedRolloutPath = null,
        string? unreadableRolloutPath = null,
        string? pendingPath = null)
    {
        return new StatusSnapshot
        {
            CodexHome = codexHome ?? paths.DefaultCodexHome,
            SqliteHome = sqliteHome ?? paths.RequiredSqliteHomeOverride!,
            SqliteHomeSource = "gui",
            CheckedStateDbPaths = checkedStateDbPath is null ? [] : [checkedStateDbPath],
            CurrentProvider = new CurrentProviderInfo("openai", false),
            ConfiguredProviders = ["openai"],
            RolloutCounts = new ProviderCounts(),
            LockedRolloutFiles = lockedRolloutPath is null ? [] : [lockedRolloutPath],
            UnreadableRolloutFiles = unreadableRolloutPath is null ? [] : [unreadableRolloutPath],
            EncryptedContentCounts = new ProviderCounts(),
            SqliteCounts = null,
            StateDbLocation = stateDbPath is null
                ? null
                : new StateDbLocation(stateDbPath, "state_5.sqlite", "sqlite-home"),
            BackupRoot = backupRoot ?? Path.Combine(paths.DefaultCodexHome, "backups_state", "provider-sync"),
            BackupSummary = new BackupSummary
            {
                Count = 0,
                TotalBytes = 0
            },
            PendingTransactions = pendingPath is null
                ? []
                : [new TransactionRecoveryInfo(
                    "operation",
                    "prepared",
                    pendingPath,
                    Path.Combine(pendingPath, "transaction.jsonl"))]
        };
    }

    private sealed class RejectingNetworkHandler : HttpMessageHandler
    {
        internal int RequestCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestCount++;
            return Task.FromException<HttpResponseMessage>(
                new InvalidOperationException("The isolated update entry attempted network access."));
        }
    }

    [Fact]
    public async Task BoundedReader_RejectsMessagesOverTheProtocolLimit()
    {
        await using MemoryStream stream = new(Encoding.UTF8.GetBytes(new string('x', 33)));
        await Assert.ThrowsAsync<InvalidDataException>(() => GuiAutomationBridge.ReadBoundedLineAsync(
            stream,
            maximumBytes: 32,
            timeout: TimeSpan.FromSeconds(1),
            cancellationToken: CancellationToken.None));
    }

    [Fact]
    public async Task WrongTokenSpendsTheSingleClientPipe_AndASecondClientCannotConnect()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        AutomationBootstrap bootstrap = new(
            true,
            fixture.WriteDescriptor(),
            fixture.Root,
            $"CodexProviderSync.Automation.{Guid.NewGuid():N}",
            new string('a', 64));
        using MainForm form = new(
            new ExecutionLogService(paths.LogDirectory),
            new SettingsService(paths.SettingsPath),
            paths: paths,
            platformBoundary: new IsolatedAppPlatformBoundary(paths));
        using GuiAutomationBridge bridge = new(form, bootstrap, paths);
        bridge.Start();

        using NamedPipeClientStream first = new(".", bootstrap.PipeName!, PipeDirection.InOut, PipeOptions.Asynchronous);
        await first.ConnectAsync(2000, CancellationToken.None);
        byte[] request = Encoding.UTF8.GetBytes(
            "{\"id\":\"one\",\"method\":\"ui.describe\",\"token\":\"" + new string('b', 64) + "\"}\n");
        await first.WriteAsync(request, CancellationToken.None);
        string? response = await GuiAutomationBridge.ReadBoundedLineAsync(
            first,
            GuiAutomationBridge.MaximumMessageBytes,
            TimeSpan.FromSeconds(2),
            CancellationToken.None);
        Assert.Contains("authentication-failed", response, StringComparison.Ordinal);
        first.Dispose();

        using NamedPipeClientStream second = new(".", bootstrap.PipeName!, PipeDirection.InOut, PipeOptions.Asynchronous);
        await Assert.ThrowsAnyAsync<Exception>(() => second.ConnectAsync(200, CancellationToken.None));
    }

    [Fact]
    public async Task AuthenticatedConnection_RejectsAReplayedRequestIdBeforeDispatch()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }
        using AutomationRoot fixture = new();
        IAppPathProvider paths = new IsolatedAppPathProvider(fixture.Root);
        string token = new('a', 64);
        AutomationBootstrap bootstrap = new(
            true,
            fixture.WriteDescriptor(),
            fixture.Root,
            $"CodexProviderSync.Automation.{Guid.NewGuid():N}",
            token);
        using MainForm form = new(
            new ExecutionLogService(paths.LogDirectory),
            new SettingsService(paths.SettingsPath),
            paths: paths,
            platformBoundary: new IsolatedAppPlatformBoundary(paths));
        using GuiAutomationBridge bridge = new(form, bootstrap, paths);
        bridge.Start();

        using NamedPipeClientStream client = new(
            ".",
            bootstrap.PipeName!,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);
        await client.ConnectAsync(2000, CancellationToken.None);
        await WriteLineAsync(
            client,
            "{\"id\":\"replay\",\"method\":\"ui.describe\",\"token\":\"" + token + "\"}");
        string? first = await GuiAutomationBridge.ReadBoundedLineAsync(
            client,
            GuiAutomationBridge.MaximumMessageBytes,
            TimeSpan.FromSeconds(2),
            CancellationToken.None);
        Assert.Contains("\"ok\":true", first, StringComparison.Ordinal);

        await WriteLineAsync(client, "{\"id\":\"replay\",\"method\":\"ui.describe\"}");
        string? replay = await GuiAutomationBridge.ReadBoundedLineAsync(
            client,
            GuiAutomationBridge.MaximumMessageBytes,
            TimeSpan.FromSeconds(2),
            CancellationToken.None);
        Assert.Contains("request-replayed", replay, StringComparison.Ordinal);
    }

    private static async Task WriteLineAsync(Stream stream, string value)
    {
        byte[] payload = Encoding.UTF8.GetBytes(value + "\n");
        await stream.WriteAsync(payload, CancellationToken.None);
        await stream.FlushAsync(CancellationToken.None);
    }

    private sealed class AutomationRoot : IDisposable
    {
        internal AutomationRoot(bool createSentinel = true)
        {
            Root = Path.Combine(Path.GetTempPath(), $"codex-provider-gui-bootstrap-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
            if (createSentinel)
            {
                File.WriteAllText(
                    Path.Combine(Root, AutomationBootstrap.SentinelFileName),
                    AutomationBootstrap.SentinelContent);
            }
        }

        internal string Root { get; }

        internal string WriteDescriptor(
            string? path = null,
            string? pipeName = null,
            string? token = null)
        {
            path ??= Path.Combine(Root, "automation.json");
            File.WriteAllText(path, JsonSerializer.Serialize(new
            {
                schemaVersion = 1,
                isolationRoot = Root,
                pipeName = pipeName ?? $"CodexProviderSync.Automation.{Guid.NewGuid():N}",
                token = token ?? Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32)).ToLowerInvariant()
            }));
            return path;
        }

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(Root, recursive: true);
            }
        }
    }
}
