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
        Assert.True(metadataRoot.GetProperty("sizeBytes").GetInt64() > 0);
        Assert.True(metadataRoot.GetProperty("fileCount").GetInt32() > 0);
        Assert.Equal(
            Directory.EnumerateFiles(backupDir, "*", SearchOption.AllDirectories)
                .Sum(static path => new FileInfo(path).Length),
            metadataRoot.GetProperty("sizeBytes").GetInt64());

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
    public async Task BackupSummary_UsesCachedInventoryAndFallsBackForLegacyMetadata()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string cached = fixture.BackupPath("20260708T091011111Z");
        string legacy = fixture.BackupPath("20260708T091011110Z");
        Directory.CreateDirectory(cached);
        Directory.CreateDirectory(legacy);
        await File.WriteAllTextAsync(
            Path.Combine(cached, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = DateTimeOffset.UtcNow,
                dbFiles = Array.Empty<string>(),
                changedSessionFiles = 0,
                sizeBytes = 123L,
                fileCount = 1
            }));
        await File.WriteAllTextAsync(Path.Combine(cached, "added-later.bin"), new string('x', 4096));
        await File.WriteAllTextAsync(
            Path.Combine(legacy, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 1,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = DateTimeOffset.UtcNow,
                dbFiles = Array.Empty<string>(),
                changedSessionFiles = 0
            }));
        await File.WriteAllTextAsync(Path.Combine(legacy, "payload.bin"), new string('y', 17));

        List<string> fallbacks = [];
        BackupService backups = new(new SessionRolloutService(), new SqliteStateService())
        {
            DirectoryInventoryFallbackObserver = fallbacks.Add
        };
        BackupSummary summary = await backups.GetBackupSummaryAsync(fixture.CodexHome);

        long legacyBytes = Directory.EnumerateFiles(legacy, "*", SearchOption.AllDirectories)
            .Sum(static path => new FileInfo(path).Length);
        Assert.Equal(2, summary.Count);
        Assert.Equal(123L + legacyBytes, summary.TotalBytes);
        Assert.Single(fallbacks);
        Assert.Equal(Path.GetFullPath(legacy), Path.GetFullPath(fallbacks[0]));
    }

    [Fact]
    public async Task IncompleteMetadata_IsNotManagedOrPrunedEvenWithProviderSyncNamespace()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string incomplete = fixture.BackupPath("20260708T091011109Z");
        Directory.CreateDirectory(incomplete);
        string sentinel = Path.Combine(incomplete, "keep.txt");
        await File.WriteAllTextAsync(
            Path.Combine(incomplete, "metadata.json"),
            "{\"namespace\":\"provider-sync\",\"sizeBytes\":1,\"fileCount\":1}");
        await File.WriteAllTextAsync(sentinel, "must remain");

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        BackupSummary summary = await backups.GetBackupSummaryAsync(fixture.CodexHome);
        BackupPruneResult pruned = await backups.PruneBackupsAsync(fixture.CodexHome, 0);

        Assert.Equal(0, summary.Count);
        Assert.Equal(0, pruned.DeletedCount);
        Assert.True(File.Exists(sentinel));
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
    public async Task RestoreBackup_NodeReducedUndoTargets_RestoresOnlyCapturedRollout()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"apigather\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-node-reduced.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-reduced", "apigather");
        string originalFirstLine = (await File.ReadAllLinesAsync(rolloutPath))[0];
        string backupDir = fixture.BackupPath("20260708T091011457Z");
        Directory.CreateDirectory(backupDir);
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.457Z",
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 1,
                undoTargets = new
                {
                    config = new { captured = false },
                    globalState = new { captured = false },
                    sqlite = new { captured = false },
                    rollout = new { captured = true, entryCount = 1 }
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
                createdAt = "2026-07-08T09:10:11.457Z",
                files = new[]
                {
                    new
                    {
                        path = rolloutPath,
                        originalFirstLine,
                        originalSeparator = "\n",
                        modelOnlyChange = false,
                        originalTurnContextModels = Array.Empty<object>()
                    }
                }
            }));

        await fixture.WriteConfigAsync("model_provider = \"manual\"");
        await fixture.WriteGlobalStateAsync(new { source = "current" });
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-reduced", "openai");

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        await backups.RestoreBackupAsync(backupDir, fixture.CodexHome, new RestoreBackupOptions());

        Assert.Equal(originalFirstLine, (await File.ReadAllLinesAsync(rolloutPath))[0]);
        Assert.Contains("manual", await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
        Assert.Contains("current", await File.ReadAllTextAsync(
            Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename)));
    }

    [Fact]
    public async Task CheckedRestore_NodeReducedUndoTargets_UsesOnlyCapturedRolloutTargets()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"apigather\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-node-reduced-checked.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-reduced-checked", "apigather");
        await fixture.WriteStateDbAsync([("thread-node-reduced-checked", "apigather", false)]);
        string originalFirstLine = (await File.ReadAllLinesAsync(rolloutPath))[0];
        string backupDir = await WriteNodeReducedRolloutBackupAsync(
            fixture,
            "20260708T091011458Z",
            rolloutPath,
            originalFirstLine);

        await fixture.WriteConfigAsync("model_provider = \"manual\"");
        await fixture.WriteGlobalStateAsync(new { source = "current" });
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-reduced-checked", "openai");
        byte[] databaseBefore = await File.ReadAllBytesAsync(fixture.StateDbPath());

        CodexSyncService service = new();
        RestoreBackupOptions options = new();
        CoreWritePlanSnapshot plan = await service.CreateRestorePlanSnapshotAsync(
            fixture.CodexHome,
            backupDir,
            options);
        Assert.DoesNotContain(plan.Targets, target =>
            string.Equals(target.Path, Path.Combine(fixture.CodexHome, "config.toml"), StringComparison.Ordinal)
            && target.Action == "restore");
        Assert.DoesNotContain(plan.Targets, target =>
            string.Equals(target.Path, fixture.StateDbPath(), StringComparison.OrdinalIgnoreCase));

        await service.RunRestoreCheckedAsync(plan, fixture.CodexHome, backupDir, options);

        Assert.Equal(originalFirstLine, (await File.ReadAllLinesAsync(rolloutPath))[0]);
        Assert.Contains("manual", await File.ReadAllTextAsync(Path.Combine(fixture.CodexHome, "config.toml")));
        Assert.Contains("current", await File.ReadAllTextAsync(
            Path.Combine(fixture.CodexHome, AppConstants.GlobalStateFileBasename)));
        Assert.Equal(databaseBefore, await File.ReadAllBytesAsync(fixture.StateDbPath()));
    }

    [Fact]
    public async Task NodeReducedUndoBackup_CannotResolveLegacyJournalTargetItDidNotCapture()
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        await fixture.WriteConfigAsync("model_provider = \"apigather\"");
        string rolloutPath = fixture.RolloutPath("sessions", "rollout-node-reduced-pending.jsonl");
        await fixture.WriteRolloutAsync(rolloutPath, "thread-node-reduced-pending", "apigather");
        await fixture.WriteStateDbAsync([("thread-node-reduced-pending", "apigather", false)]);
        string backupDir = await WriteNodeReducedRolloutBackupAsync(
            fixture,
            "20260708T091011459Z",
            rolloutPath,
            (await File.ReadAllLinesAsync(rolloutPath))[0]);
        await using FileTransactionJournal journal = await FileTransactionJournal.CreateAsync(
            backupDir,
            fixture.CodexHome,
            "openai",
            [fixture.StateDbPath()]);
        await journal.ApplyingAsync("sqlite", fixture.StateDbPath());

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new CodexSyncService().RunRestoreAsync(
                fixture.CodexHome,
                backupDir,
                new RestoreBackupOptions()));

        Assert.Contains("SQLite", error.Message, StringComparison.Ordinal);
        Assert.Single(await FileTransactionJournal.FindPendingAsync(fixture.CodexHome));
    }

    [Theory]
    [InlineData("config")]
    [InlineData("globalState")]
    [InlineData("sqlite")]
    [InlineData("rollout")]
    public async Task NodeReducedUndoBackup_MissingTargetDeclarationFailsClosed(string missingTarget)
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string backupDir = fixture.BackupPath("20260708T091011460Z");
        Directory.CreateDirectory(backupDir);
        Dictionary<string, object> undoTargets = new()
        {
            ["config"] = new { captured = false },
            ["globalState"] = new { captured = false },
            ["sqlite"] = new { captured = false },
            ["rollout"] = new { captured = false }
        };
        undoTargets.Remove(missingTarget);
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.460Z",
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 0,
                undoTargets
            }));

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            backups.RestoreBackupAsync(backupDir, fixture.CodexHome, new RestoreBackupOptions()));

        Assert.Contains($"required target {missingTarget}", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"captured\":\"false\"}")]
    public async Task NodeReducedUndoBackup_MalformedCapturedDeclarationFailsClosed(string configTarget)
    {
        TestCodexHomeFixture fixture = await TestCodexHomeFixture.CreateAsync();
        string backupDir = fixture.BackupPath("20260708T091011461Z");
        Directory.CreateDirectory(backupDir);
        string metadata = $$"""
            {
              "version": 2,
              "namespace": "{{AppConstants.BackupNamespace}}",
              "codexHome": {{JsonSerializer.Serialize(fixture.CodexHome)}},
              "targetProvider": "openai",
              "dbFiles": [],
              "sqliteDbFiles": [],
              "changedSessionFiles": 0,
              "undoTargets": {
                "config": {{configTarget}},
                "globalState": { "captured": false },
                "sqlite": { "captured": false },
                "rollout": { "captured": false }
              }
            }
            """;
        await File.WriteAllTextAsync(Path.Combine(backupDir, "metadata.json"), metadata);

        BackupService backups = new(new SessionRolloutService(), new SqliteStateService());
        await Assert.ThrowsAnyAsync<Exception>(() =>
            backups.RestoreBackupAsync(backupDir, fixture.CodexHome, new RestoreBackupOptions()));
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

    private static async Task<string> WriteNodeReducedRolloutBackupAsync(
        TestCodexHomeFixture fixture,
        string backupName,
        string rolloutPath,
        string originalFirstLine)
    {
        string backupDir = fixture.BackupPath(backupName);
        Directory.CreateDirectory(backupDir);
        await File.WriteAllTextAsync(
            Path.Combine(backupDir, "metadata.json"),
            JsonSerializer.Serialize(new
            {
                version = 2,
                @namespace = AppConstants.BackupNamespace,
                codexHome = fixture.CodexHome,
                targetProvider = "openai",
                createdAt = "2026-07-08T09:10:11.458Z",
                dbFiles = Array.Empty<string>(),
                sqliteDbFiles = Array.Empty<string>(),
                changedSessionFiles = 1,
                undoTargets = new
                {
                    config = new { captured = false },
                    globalState = new { captured = false },
                    sqlite = new { captured = false },
                    rollout = new { captured = true, entryCount = 1 }
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
                createdAt = "2026-07-08T09:10:11.458Z",
                files = new[]
                {
                    new
                    {
                        path = rolloutPath,
                        originalFirstLine,
                        originalSeparator = "\n",
                        modelOnlyChange = false,
                        originalTurnContextModels = Array.Empty<object>()
                    }
                }
            }));
        return backupDir;
    }
}
