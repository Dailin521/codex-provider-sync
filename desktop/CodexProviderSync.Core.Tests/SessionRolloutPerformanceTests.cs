using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Xunit.Abstractions;

namespace CodexProviderSync.Core.Tests;

public sealed class SessionRolloutPerformanceTests(ITestOutputHelper output)
{
    [Fact]
    public async Task ProviderChange_ContentFingerprintMatchesExactRolloutBytes()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        try
        {
            string path = fixture.RolloutPath("sessions", "rollout-fingerprint.jsonl");
            await fixture.WriteRolloutAsync(path, "thread-fingerprint", "apigather");
            FileInfo snapshot = new(path);
            string digest = Convert.ToHexString(
                    SHA256.HashData(await File.ReadAllBytesAsync(path)))
                .ToLowerInvariant();

            SessionChangeCollection result = await new SessionRolloutService()
                .CollectSessionChangesAsync(fixture.CodexHome, "openai");

            SessionChange change = Assert.Single(result.Changes);
            Assert.Equal(
                $"sha256:{digest}:{snapshot.Length}:{snapshot.LastWriteTimeUtc.Ticks}",
                change.ContentFingerprint);
        }
        finally
        {
            Directory.Delete(fixture.Root, recursive: true);
        }
    }

    [Fact]
    public async Task CollectSessionChanges_UsesOneContentPassPerRollout()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        try
        {
            for (int index = 0; index < 16; index++)
            {
                string directory = index < 12 ? "sessions" : "archived_sessions";
                string path = fixture.RolloutPath(directory, $"rollout-{index:D2}.jsonl");
                await fixture.WriteRolloutAsync(path, $"thread-{index:D2}", "apigather");
            }

            SessionChangeCollection result = await new SessionRolloutService()
                .CollectSessionChangesAsync(fixture.CodexHome, "openai");

            Assert.Equal(16, result.ScanMetrics.EnumeratedRolloutFiles);
            Assert.Equal(16, result.ScanMetrics.ParsedSessionFiles);
            Assert.Equal(16, result.ScanMetrics.ContentScanPasses);
            Assert.Equal(0, result.ScanMetrics.ModelScanFiles);
            Assert.Equal(16, result.Changes.Count);
            Assert.Equal(16, result.UserEventThreadIds.Count);
        }
        finally
        {
            Directory.Delete(fixture.Root, recursive: true);
        }
    }

    [Fact]
    public async Task CollectSessionChanges_CollectsAllModelBackupsInTheSameContentPass()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        try
        {
            for (int index = 0; index < 8; index++)
            {
                string path = fixture.RolloutPath("sessions", $"rollout-model-{index:D2}.jsonl");
                await fixture.WriteRolloutWithTurnContextAsync(
                    path,
                    $"thread-model-{index:D2}",
                    "openai",
                    "old-model");
            }

            SessionChangeCollection result = await new SessionRolloutService()
                .CollectSessionChangesAsync(
                    fixture.CodexHome,
                    "openai",
                    targetModel: "new-model");

            Assert.Equal(8, result.ScanMetrics.EnumeratedRolloutFiles);
            Assert.Equal(8, result.ScanMetrics.ContentScanPasses);
            Assert.Equal(8, result.ScanMetrics.ModelScanFiles);
            Assert.Equal(8, result.Changes.Count);
            Assert.All(result.Changes, change => Assert.Equal(2, change.OriginalTurnContextModels.Count));
        }
        finally
        {
            Directory.Delete(fixture.Root, recursive: true);
        }
    }

    [Fact]
    [Trait("Category", "Performance")]
    public async Task EightHundredRollouts_UseExactlyEightHundredContentPasses()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("CODEX_PROVIDER_SYNC_RUN_PERF_TESTS"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        try
        {
            string sessionDirectory = Path.GetDirectoryName(
                fixture.RolloutPath("sessions", "rollout-placeholder.jsonl"))!;
            for (int index = 0; index < 800; index++)
            {
                string first = JsonSerializer.Serialize(new
                {
                    timestamp = "2026-08-07T00:00:00.000Z",
                    type = "session_meta",
                    payload = new
                    {
                        id = $"thread-{index:D4}",
                        cwd = "C:\\AITemp",
                        model_provider = "apigather"
                    }
                });
                string user = JsonSerializer.Serialize(new
                {
                    type = "event_msg",
                    payload = new { type = "user_message", message = "hi" }
                });
                await File.WriteAllTextAsync(
                    Path.Combine(sessionDirectory, $"rollout-{index:D4}.jsonl"),
                    $"{first}\n{user}\n");
            }

            Stopwatch timer = Stopwatch.StartNew();
            SessionChangeCollection result = await new SessionRolloutService()
                .CollectSessionChangesAsync(fixture.CodexHome, "openai");
            timer.Stop();

            Assert.Equal(800, result.ScanMetrics.EnumeratedRolloutFiles);
            Assert.Equal(800, result.ScanMetrics.ContentScanPasses);
            Assert.Equal(0, result.ScanMetrics.ModelScanFiles);
            output.WriteLine(
                $"rollouts=800 elapsedMs={timer.ElapsedMilliseconds} contentPasses={result.ScanMetrics.ContentScanPasses}");
        }
        finally
        {
            Directory.Delete(fixture.Root, recursive: true);
        }
    }
}
