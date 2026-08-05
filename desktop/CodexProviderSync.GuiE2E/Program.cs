using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E;

internal sealed record GuiE2EOptions(
    string ExePath,
    string ManifestPath,
    string ScenarioAssetPath,
    string EvidencePath,
    string IsolationRoot,
    TimeSpan Timeout)
{
    internal static GuiE2EOptions Parse(string[] args)
    {
        Dictionary<string, string> values = new(StringComparer.Ordinal);
        for (int index = 0; index < args.Length; index += 2)
        {
            if (index + 1 >= args.Length || !args[index].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException("GUI E2E options must be supplied as --name value pairs.");
            }
            values.Add(args[index], args[index + 1]);
        }
        string Required(string name) => values.TryGetValue(name, out string? value) && !string.IsNullOrWhiteSpace(value)
            ? Path.GetFullPath(value)
            : throw new ArgumentException($"Missing required option {name}.");
        int timeoutSeconds = values.TryGetValue("--timeout-seconds", out string? timeoutValue)
            ? int.Parse(timeoutValue, System.Globalization.CultureInfo.InvariantCulture)
            : 90;
        if (timeoutSeconds is < 15 or > 600)
        {
            throw new ArgumentOutOfRangeException(nameof(args), "--timeout-seconds must be between 15 and 600.");
        }
        return new(
            Required("--exe"),
            Required("--manifest"),
            Required("--scenarios"),
            Required("--evidence"),
            Required("--root"),
            TimeSpan.FromSeconds(timeoutSeconds));
    }
}

internal static class Program
{
    [STAThread]
    private static async Task<int> Main(string[] args)
    {
        EvidenceDocument evidence = new();
        GuiE2EOptions? options = null;
        ScenarioRunner? runner = null;
        CancellationTokenSource? runWatchdog = null;
        string? token = null;
        try
        {
            options = GuiE2EOptions.Parse(args);
            ValidateInputs(options);
            TimeSpan wholeRunTimeout = GuiE2EWatchdog.WholeRunTimeout(options.Timeout);
            runWatchdog = new CancellationTokenSource(wholeRunTimeout);
            CancellationToken runToken = runWatchdog.Token;
            DesktopProbe desktop = NativeWindows.ProbeInteractiveDesktop();
            evidence.Environment["operatingSystem"] = Environment.OSVersion.VersionString;
            evidence.Environment["userInteractive"] = Environment.UserInteractive;
            evidence.Environment["activeConsoleSessionId"] = desktop.ActiveSessionId;
            evidence.Environment["processSessionId"] = desktop.ProcessSessionId;
            evidence.Environment["inputDesktop"] = desktop.DesktopName;
            evidence.Environment["desktopProbe"] = desktop.Message;
            evidence.Environment["requestTimeoutSeconds"] = options.Timeout.TotalSeconds;
            evidence.Environment["wholeRunTimeoutSeconds"] = wholeRunTimeout.TotalSeconds;
            if (!desktop.Passed)
            {
                throw new InvalidOperationException(
                    "Headful Windows GUI E2E requires the active interactive desktop; this is a FAIL, never a skip. "
                    + desktop.Message);
            }

            IsolatedFixture fixture = new(options.IsolationRoot);
            await fixture.InitializeAsync(runToken);
            IReadOnlyDictionary<string, string?> sourceEnvironment = Environment.GetEnvironmentVariables()
                .Cast<System.Collections.DictionaryEntry>()
                .ToDictionary(entry => (string)entry.Key, entry => entry.Value?.ToString(), StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string?> childEnvironment = IsolationEnvironment.Build(fixture.Root, sourceEnvironment);
            foreach (string name in new[] { "HOME", "USERPROFILE", "CODEX_HOME", "CODEX_SQLITE_HOME", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP" })
            {
                if (!IsolationEnvironment.IsContained(fixture.Root, childEnvironment[name]!))
                {
                    throw new InvalidOperationException($"Sanitized child environment escaped the isolation root: {name}.");
                }
            }
            evidence.Environment["isolationRoot"] = fixture.Root;
            evidence.Environment["sanitizedDirectories"] = childEnvironment
                .Where(pair => new[] { "HOME", "USERPROFILE", "CODEX_HOME", "CODEX_SQLITE_HOME", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP" }
                    .Contains(pair.Key, StringComparer.OrdinalIgnoreCase))
                .ToDictionary(pair => pair.Key, pair => pair.Value);

            evidence.Executable["path"] = options.ExePath;
            evidence.Executable["sha256"] = await Hashing.Sha256FileAsync(options.ExePath, runToken);
            evidence.Executable["releasePublished"] = true;

            string manifestJson = await File.ReadAllTextAsync(options.ManifestPath, runToken);
            string scenarioJson = await File.ReadAllTextAsync(options.ScenarioAssetPath, runToken);
            evidence.Manifest["path"] = options.ManifestPath;
            evidence.Manifest["sha256"] = Hashing.Sha256Text(manifestJson);
            evidence.Manifest["scenarioAssetSha256"] = Hashing.Sha256Text(scenarioJson);

            FixtureSnapshot before = await fixture.SnapshotAsync(runToken);
            runner = new ScenarioRunner(options, fixture, evidence, childEnvironment, manifestJson, scenarioJson);
            token = await runner.RunAsync(before, runToken);

            evidence.Passed = evidence.Errors.Count == 0
                && evidence.Blockers.Count == 0
                && evidence.Scenarios.All(item => string.Equals(item["status"] as string, "passed", StringComparison.Ordinal));
            evidence.FinishedAtUtc = DateTimeOffset.UtcNow;
            await WriteEvidenceAsync(options.EvidencePath, evidence, token);
            Console.WriteLine($"GUI_E2E_EVIDENCE={options.EvidencePath}");
            Console.WriteLine($"GUI_E2E_RESULT={(evidence.Passed ? "PASS" : "FAIL")}");
            return evidence.Passed ? 0 : 1;
        }
        catch (Exception error)
        {
            evidence.Errors.Add(error.ToString());
            if (runWatchdog?.IsCancellationRequested == true)
            {
                evidence.Blockers.Add("The whole-run GUI E2E watchdog expired before the harness completed.");
            }
            if (options is not null)
            {
                try
                {
                    using CancellationTokenSource cleanup = new(
                        GuiE2EWatchdog.FailureFinalizationTimeout(options.Timeout));
                    if (runner is null && File.Exists(options.ManifestPath) && File.Exists(options.ScenarioAssetPath))
                    {
                        string manifestJson = await File.ReadAllTextAsync(options.ManifestPath, cleanup.Token);
                        string scenarioJson = await File.ReadAllTextAsync(options.ScenarioAssetPath, cleanup.Token);
                        IsolatedFixture fixture = new(options.IsolationRoot);
                        IReadOnlyDictionary<string, string?> sourceEnvironment = Environment.GetEnvironmentVariables()
                            .Cast<System.Collections.DictionaryEntry>()
                            .ToDictionary(
                                entry => (string)entry.Key,
                                entry => entry.Value?.ToString(),
                                StringComparer.OrdinalIgnoreCase);
                        runner = new ScenarioRunner(
                            options,
                            fixture,
                            evidence,
                            IsolationEnvironment.Build(fixture.Root, sourceEnvironment),
                            manifestJson,
                            scenarioJson);
                    }
                    if (runner is not null)
                    {
                        await runner.FinalizeFailureEvidenceAsync(cleanup.Token);
                    }
                }
                catch (Exception finalizationError)
                {
                    evidence.Errors.Add($"Failure evidence finalization also failed: {finalizationError}");
                }
            }
            evidence.Passed = false;
            evidence.FinishedAtUtc = DateTimeOffset.UtcNow;
            if (options is not null)
            {
                try
                {
                    await WriteEvidenceAsync(options.EvidencePath, evidence, token);
                    Console.Error.WriteLine($"GUI_E2E_EVIDENCE={options.EvidencePath}");
                }
                catch (Exception evidenceError)
                {
                    Console.Error.WriteLine($"Unable to write GUI E2E evidence: {evidenceError.Message}");
                }
            }
            Console.Error.WriteLine(error);
            Console.Error.WriteLine("GUI_E2E_RESULT=FAIL");
            return 1;
        }
        finally
        {
            runWatchdog?.Dispose();
        }
    }

    private static void ValidateInputs(GuiE2EOptions options)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("Real WinForms E2E can only run on Windows.");
        }
        if (!File.Exists(options.ExePath)
            || !string.Equals(Path.GetFileName(options.ExePath), "CodexProviderSync.exe", StringComparison.OrdinalIgnoreCase))
        {
            throw new FileNotFoundException("A published CodexProviderSync.exe is required.", options.ExePath);
        }
        if (!File.Exists(options.ManifestPath) || !File.Exists(options.ScenarioAssetPath))
        {
            throw new FileNotFoundException("Published manifest and scenario asset are required.");
        }
        if (Directory.Exists(options.IsolationRoot) && Directory.EnumerateFileSystemEntries(options.IsolationRoot).Any())
        {
            throw new InvalidOperationException("Isolation root must be new and empty.");
        }
        string evidenceDirectory = Path.GetDirectoryName(options.EvidencePath)
            ?? throw new InvalidOperationException("Evidence path must have a parent directory.");
        Directory.CreateDirectory(evidenceDirectory);
    }

    private static async Task WriteEvidenceAsync(string path, EvidenceDocument evidence, string? token)
    {
        string directory = Path.GetDirectoryName(path)
            ?? throw new InvalidOperationException("Evidence path must have a parent directory.");
        Directory.CreateDirectory(directory);
        JsonSerializerOptions options = new(JsonSerializerDefaults.Web) { WriteIndented = true };
        string json = JsonSerializer.Serialize(evidence, options);
        string redacted = EvidenceRedactor.Redact(json, token is null ? [] : [token]);
        await File.WriteAllTextAsync(path, redacted, new UTF8Encoding(false));
    }
}

internal static class GuiE2EWatchdog
{
    internal static TimeSpan WholeRunTimeout(TimeSpan requestTimeout)
    {
        if (requestTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(requestTimeout));
        }
        return TimeSpan.FromSeconds(Math.Clamp(requestTimeout.TotalSeconds * 6, 240, 2700));
    }

    internal static TimeSpan FailureFinalizationTimeout(TimeSpan requestTimeout)
    {
        if (requestTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(requestTimeout));
        }
        return TimeSpan.FromSeconds(Math.Clamp(requestTimeout.TotalSeconds, 5, 30));
    }
}

internal sealed class AppSession : IAsyncDisposable
{
    private AppSession(Process process, GuiBridgeClient bridge, nint mainWindow, string token)
    {
        Process = process;
        Bridge = bridge;
        MainWindow = mainWindow;
        Token = token;
    }

    internal Process Process { get; }
    internal GuiBridgeClient Bridge { get; }
    internal nint MainWindow { get; }
    internal string Token { get; }

    internal static async Task<AppSession> StartAsync(
        GuiE2EOptions options,
        IsolatedFixture fixture,
        IReadOnlyDictionary<string, string?> environment,
        int generation,
        CancellationToken cancellationToken)
    {
        AutomationLaunchDescriptor descriptor = fixture.CreateDescriptor(generation);
        ProcessStartInfo startInfo = new()
        {
            FileName = options.ExePath,
            UseShellExecute = false,
            CreateNoWindow = false,
            WorkingDirectory = Path.GetDirectoryName(options.ExePath)!
        };
        startInfo.ArgumentList.Add("--gui-automation-descriptor");
        startInfo.ArgumentList.Add(fixture.DescriptorPath(generation));
        startInfo.Environment.Clear();
        foreach ((string name, string? value) in environment)
        {
            if (value is not null)
            {
                startInfo.Environment[name] = value;
            }
        }
        Process process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Unable to launch the published GUI executable.");
        try
        {
            Task<nint> windowTask = NativeWindows.WaitForVisibleMainWindowAsync(process, options.Timeout, cancellationToken);
            Task<GuiBridgeClient> bridgeTask = GuiBridgeClient.ConnectAsync(
                descriptor.PipeName,
                descriptor.Token,
                options.Timeout,
                cancellationToken);
            await Task.WhenAll(windowTask, bridgeTask);
            GuiBridgeClient bridge = await bridgeTask;
            nint window = await windowTask;
            return new AppSession(process, bridge, window, descriptor.Token);
        }
        catch
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
            process.Dispose();
            throw;
        }
    }

    internal async Task ShutdownAsync(CancellationToken cancellationToken)
    {
        if (!Process.HasExited)
        {
            BridgeResponse response = await Bridge.SendAsync("ui.shutdown", cancellationToken: cancellationToken);
            if (!response.Ok)
            {
                throw new InvalidOperationException($"GUI shutdown was rejected: {response.ErrorMessage}");
            }
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(15));
            await Process.WaitForExitAsync(timeout.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Bridge.DisposeAsync();
        if (!Process.HasExited)
        {
            Process.Kill(entireProcessTree: true);
            await Process.WaitForExitAsync();
        }
        Process.Dispose();
    }
}
