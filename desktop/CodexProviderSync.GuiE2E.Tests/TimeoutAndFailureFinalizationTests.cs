using System.Diagnostics;
using System.IO.Pipes;
using System.Text;
using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class TimeoutAndFailureFinalizationTests
{
    [Fact]
    public async Task SendAsync_TimesOutAStalledResponse_WithoutOverReleasingQueuedRequestLock()
    {
        string pipeName = $"codex-provider-sync-timeout-{Guid.NewGuid():N}";
        await using NamedPipeServerStream server = new(
            pipeName,
            PipeDirection.InOut,
            1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        Task serverConnected = server.WaitForConnectionAsync();
        Task<GuiBridgeClient> clientConnected = GuiBridgeClient.ConnectAsync(
            pipeName,
            "test-token",
            TimeSpan.FromMilliseconds(750),
            CancellationToken.None);
        await Task.WhenAll(serverConnected, clientConnected);
        await using GuiBridgeClient client = await clientConnected;
        using StreamReader serverReader = new(server, new UTF8Encoding(false), leaveOpen: true);

        Stopwatch elapsed = Stopwatch.StartNew();
        Task<BridgeResponse> stalled = client.SendAsync("ui.stalled");
        Assert.NotNull(await serverReader.ReadLineAsync());

        using CancellationTokenSource queuedCancellation = new();
        Task<BridgeResponse> queued = client.SendAsync(
            "ui.queued",
            cancellationToken: queuedCancellation.Token);
        queuedCancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
        TimeoutException timeout = await Assert.ThrowsAsync<TimeoutException>(() => stalled);
        elapsed.Stop();

        Assert.Contains("ui.stalled", timeout.Message, StringComparison.Ordinal);
        Assert.InRange(elapsed.Elapsed, TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(3));
    }

    [Theory]
    [InlineData(15, 240)]
    [InlineData(90, 540)]
    [InlineData(600, 2700)]
    public void WholeRunWatchdog_IsWiderThanARequest_AndBounded(int requestSeconds, int expectedSeconds)
    {
        TimeSpan request = TimeSpan.FromSeconds(requestSeconds);
        TimeSpan wholeRun = GuiE2EWatchdog.WholeRunTimeout(request);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), wholeRun);
        Assert.True(wholeRun > request);
    }

    [Fact]
    public void Watchdog_RejectsNonPositiveTimeouts()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => GuiE2EWatchdog.WholeRunTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => GuiE2EWatchdog.FailureFinalizationTimeout(TimeSpan.Zero));
    }

    [Fact]
    public async Task FailureFinalization_IsIdempotent_CollectsPartialTrace_AndMaterializesAllRequiredScenarios()
    {
        string repositoryRoot = FindRepositoryRoot();
        string parent = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-finalization-tests",
            Guid.NewGuid().ToString("N"));
        string isolationRoot = Path.Combine(parent, "isolation");
        Directory.CreateDirectory(parent);
        try
        {
            string manifestPath = Path.Combine(
                repositoryRoot,
                "desktop",
                "CodexProviderSync.App",
                "Automation",
                "gui-automation-manifest.v0.4.json");
            string scenarioPath = Path.Combine(
                repositoryRoot,
                "desktop",
                "CodexProviderSync.GuiE2E",
                "assets",
                "gui-e2e-scenarios.v0.4.json");
            string manifestJson = await File.ReadAllTextAsync(manifestPath);
            string scenarioJson = await File.ReadAllTextAsync(scenarioPath);
            IsolatedFixture fixture = new(isolationRoot);
            Directory.CreateDirectory(Path.GetDirectoryName(fixture.TracePath)!);
            const string firstTrace = "{\"requestId\":\"request-1\",\"eventObserved\":true}";
            const string secondTrace = "{\"requestId\":\"request-2\",\"eventObserved\":false}";
            await File.WriteAllLinesAsync(fixture.TracePath, [firstTrace, "not-json", secondTrace]);

            EvidenceDocument evidence = new();
            evidence.Trace.Add(JsonNode.Parse(firstTrace));
            GuiE2EOptions options = new(
                Path.Combine(parent, "CodexProviderSync.exe"),
                manifestPath,
                scenarioPath,
                Path.Combine(parent, "evidence.json"),
                isolationRoot,
                TimeSpan.FromSeconds(30));
            ScenarioRunner runner = new(
                options,
                fixture,
                evidence,
                new Dictionary<string, string?>(),
                manifestJson,
                scenarioJson);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));

            await runner.FinalizeFailureEvidenceAsync(timeout.Token);
            await runner.FinalizeFailureEvidenceAsync(timeout.Token);

            Assert.Equal(53, evidence.Scenarios.Count);
            Assert.Equal(53, evidence.Scenarios
                .Select(item => Assert.IsType<string>(item["id"]))
                .Distinct(StringComparer.Ordinal)
                .Count());
            Assert.All(evidence.Scenarios, item => Assert.Equal("blocked", item["status"]));
            Assert.Equal(2, evidence.Trace.Count);
            Assert.Equal(1, evidence.Manifest["failureTraceMalformedLineCount"]);
            Assert.Equal(53, evidence.Manifest["requiredHeadfulScenarioCount"]);
            Assert.Equal(0, evidence.Manifest["passedRequiredHeadfulScenarioCount"]);
            Assert.Equal(false, evidence.Manifest["coveragePassed"]);
            Assert.Single(evidence.Blockers);
        }
        finally
        {
            if (Directory.Exists(parent))
            {
                Directory.Delete(parent, recursive: true);
            }
        }
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            string manifestPath = Path.Combine(
                directory.FullName,
                "desktop",
                "CodexProviderSync.App",
                "Automation",
                "gui-automation-manifest.v0.4.json");
            if (File.Exists(manifestPath))
            {
                return directory.FullName;
            }
            directory = directory.Parent;
        }
        throw new DirectoryNotFoundException("Repository root was not found from the test output path.");
    }
}
