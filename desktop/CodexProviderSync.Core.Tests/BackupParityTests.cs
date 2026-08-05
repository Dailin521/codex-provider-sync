using System.Text.Json;

namespace CodexProviderSync.Core.Tests;

public sealed class BackupParityTests
{
    [Fact]
    public async Task CreateBackup_WritesCanonicalCrossRuntimeMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-backup-parity.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-backup-parity", "apigather");
        DateTimeOffset originalTimestamp = new(2026, 7, 8, 9, 10, 11, 123, TimeSpan.Zero);
        File.SetLastWriteTimeUtc(rolloutPath, originalTimestamp.UtcDateTime);
        await File.WriteAllTextAsync(
            Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename),
            "{\"source\":\"csharp\"}\n");

        SessionRolloutService rollouts = new();
        SessionChangeCollection changes = await rollouts.CollectSessionChangesAsync(
            fixture.CodexHome,
            "openai");
        BackupService backups = new(rollouts, new SqliteStateService());
        string backupDir = await backups.CreateBackupAsync(
            fixture.CodexHome,
            "openai",
            changes.Changes,
            Path.Combine(fixture.CodexHome, "config.toml"));

        using JsonDocument metadata = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(backupDir, "metadata.json")));
        JsonElement metadataRoot = metadata.RootElement;
        JsonElement globalStateFiles = metadataRoot.GetProperty("globalStateFiles");
        Assert.True(globalStateFiles.GetProperty(AppConstants.GlobalStateFileBasename).GetBoolean());
        Assert.False(globalStateFiles.GetProperty(AppConstants.GlobalStateBackupFileBasename).GetBoolean());
        Assert.True(metadataRoot.GetProperty("globalStateFilePresent").GetBoolean());
        Assert.False(metadataRoot.GetProperty("globalStateBackupFilePresent").GetBoolean());

        using JsonDocument manifest = JsonDocument.Parse(
            await File.ReadAllTextAsync(Path.Combine(backupDir, "session-meta-backup.json")));
        JsonElement entry = Assert.Single(manifest.RootElement.GetProperty("files").EnumerateArray());
        Assert.Equal("2026-07-08T09:10:11.123Z", entry.GetProperty("originalLastWriteTimeUtc").GetString());
        Assert.Equal(originalTimestamp.ToUnixTimeMilliseconds(), entry.GetProperty("originalMtimeMs").GetInt64());
        Assert.Equal(JsonValueKind.String, entry.GetProperty("originalLastWriteTimeUtcTicks").ValueKind);
        Assert.Equal(
            originalTimestamp.UtcTicks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            entry.GetProperty("originalLastWriteTimeUtcTicks").GetString());
    }

    [Fact]
    public async Task RestoreBackup_AcceptsNodeStyleV2MetadataAndMillisecondSessionTimestamp()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"openai\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-node-backup.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-backup", "apigather");
        string originalFirstLine = (await File.ReadAllLinesAsync(rolloutPath))[0];
        DateTimeOffset originalTimestamp = new(2026, 7, 8, 9, 10, 11, 456, TimeSpan.Zero);

        string backupDir = fixture.BackupPath("20260708T091011456Z");
        Directory.CreateDirectory(backupDir);
        await File.WriteAllTextAsync(Path.Combine(backupDir, "config.toml"), "model_provider = \"apigather\"\n");
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, AppConstants.GlobalStateFileBasename),
            "{\"source\":\"node-backup\"}\n");
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                sqliteHome = Path.Combine(fixture.CodexHome, AppConstants.SqliteDirBasename),
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.456Z",
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 1,
                globalStateFiles = new Dictionary<string, bool>
                {
                    [AppConstants.GlobalStateFileBasename] = true,
                    [AppConstants.GlobalStateBackupFileBasename] = false
                }
            }));
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "session-meta-backup.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.456Z",
                files = new[]
                {
                    new
                    {
                        path = rolloutPath,
                        originalFirstLine,
                        originalSeparator = "\n",
                        originalLastWriteTimeUtc = "2026-07-08T09:10:11.456Z",
                        originalMtimeMs = originalTimestamp.ToUnixTimeMilliseconds(),
                        modelOnlyChange = false,
                        originalTurnContextModels = Array.Empty<object>()
                    }
                }
            }));

        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-backup", "openai");
        string statePath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename);
        string stateBackupPath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateBackupFileBasename);
        await File.WriteAllTextAsync(statePath, "{\"source\":\"current\"}\n");
        await File.WriteAllTextAsync(stateBackupPath, "created-after-backup\n");

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        await backups.RestoreBackupAsync(
            backupDir,
            fixture.CodexHome,
            new RestoreBackupOptions { RestoreDatabase = false });

        Assert.Equal(originalFirstLine, (await File.ReadAllLinesAsync(rolloutPath))[0]);
        Assert.Equal(originalTimestamp.UtcDateTime, File.GetLastWriteTimeUtc(rolloutPath));
        Assert.Equal("{\"source\":\"node-backup\"}\n", await File.ReadAllTextAsync(statePath));
        Assert.False(File.Exists(stateBackupPath));
    }

    [Fact]
    public async Task RestoreGlobalState_RejectsCanonicalAndLegacyPresenceConflictBeforeMutation()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string backupDir = fixture.BackupPath("20260708T091011999Z");
        Directory.CreateDirectory(backupDir);
        string statePath = Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename);
        await File.WriteAllTextAsync(statePath, "current\n");
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, AppConstants.GlobalStateFileBasename),
            "backup\n");
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.999Z",
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 0,
                globalStateFiles = new Dictionary<string, bool>
                {
                    [AppConstants.GlobalStateFileBasename] = true,
                    [AppConstants.GlobalStateBackupFileBasename] = true
                },
                globalStateFilePresent = true,
                globalStateBackupFilePresent = false
            }));

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => backups.RestoreGlobalStateFilesAsync(backupDir, fixture.CodexHome));

        Assert.Contains("disagrees", error.Message, StringComparison.Ordinal);
        Assert.Equal("current\n", await File.ReadAllTextAsync(statePath));
    }
}
