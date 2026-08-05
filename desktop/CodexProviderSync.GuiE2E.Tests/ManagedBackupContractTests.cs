namespace CodexProviderSync.GuiE2E.Tests;

public sealed class ManagedBackupContractTests
{
    [Fact]
    public void PruneContract_RequiresEveryRemovedManagedDirectoryToBeAbsent()
    {
        string root = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-prune-removal-tests",
            Guid.NewGuid().ToString("N"));
        string oldBackup = Path.Combine(root, "20260804T010000000Z");
        string newestBackup = Path.Combine(root, "20260804T020000000Z");
        Directory.CreateDirectory(oldBackup);
        Directory.CreateDirectory(newestBackup);
        try
        {
            Directory.Delete(oldBackup);
            PruneRemovalEvidence verified = PruneEvidenceContract.VerifyManagedRemoval(
                [oldBackup, newestBackup], [newestBackup], newestBackup);
            Assert.Equal([oldBackup], verified.RemovedPaths);

            Directory.CreateDirectory(oldBackup);
            Assert.Throws<InvalidDataException>(() => PruneEvidenceContract.VerifyManagedRemoval(
                [oldBackup, newestBackup], [newestBackup], newestBackup));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PruneContract_RequiresNonManagedSentinelContentToRemainExact()
    {
        string root = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-prune-sentinel-tests",
            Guid.NewGuid().ToString("N"));
        string sentinel = Path.Combine(root, "non-managed", "must-survive.txt");
        Directory.CreateDirectory(Path.GetDirectoryName(sentinel)!);
        try
        {
            await File.WriteAllTextAsync(sentinel, "original", CancellationToken.None);
            string expected = await Hashing.Sha256FileAsync(sentinel, CancellationToken.None);
            await PruneEvidenceContract.VerifySentinelAsync(sentinel, expected, CancellationToken.None);

            await File.WriteAllTextAsync(sentinel, "changed", CancellationToken.None);
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                PruneEvidenceContract.VerifySentinelAsync(sentinel, expected, CancellationToken.None));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ExclusiveConfigLock_IsDeterministicAndFullyRecoverable()
    {
        string parent = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-config-lock-tests",
            Guid.NewGuid().ToString("N"));
        string root = Path.Combine(parent, "isolation");
        Directory.CreateDirectory(parent);
        try
        {
            IsolatedFixture fixture = new(root);
            await fixture.InitializeAsync(CancellationToken.None);
            await using (FileStream configLock = new(
                fixture.ConfigPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None,
                4096,
                FileOptions.Asynchronous))
            {
                await Assert.ThrowsAnyAsync<IOException>(() =>
                    File.ReadAllTextAsync(fixture.ConfigPath, CancellationToken.None));
            }

            string restored = await File.ReadAllTextAsync(fixture.ConfigPath, CancellationToken.None);
            Assert.Contains("model_provider = \"openai\"", restored, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(parent))
            {
                Directory.Delete(parent, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Fixture_ProvidesValidAlternatePickerTargets_AndCountsOnlyManagedBackups()
    {
        string parent = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-managed-backup-tests",
            Guid.NewGuid().ToString("N"));
        string root = Path.Combine(parent, "isolation");
        Directory.CreateDirectory(parent);
        try
        {
            IsolatedFixture fixture = new(root);
            await fixture.InitializeAsync(CancellationToken.None);

            Assert.True(File.Exists(Path.Combine(fixture.PickerCodexHome, "config.toml")));
            Assert.True(File.Exists(Path.Combine(fixture.PickerSqliteHome, "state_5.sqlite")));

            string backupRoot = Path.Combine(fixture.CodexHome, "backups_state", "provider-sync");
            string managed = Path.Combine(backupRoot, "20260804T010000000Z");
            string wrongNamespace = Path.Combine(backupRoot, "20260804T020000000Z");
            string malformed = Path.Combine(backupRoot, "20260804T030000000Z");
            string unmanaged = Path.Combine(backupRoot, "non-managed-sentinel");
            foreach (string directory in new[] { managed, wrongNamespace, malformed, unmanaged })
            {
                Directory.CreateDirectory(directory);
            }
            await File.WriteAllTextAsync(
                Path.Combine(managed, "metadata.json"),
                "{\"namespace\":\"provider-sync\"}",
                CancellationToken.None);
            await File.WriteAllTextAsync(
                Path.Combine(wrongNamespace, "metadata.json"),
                "{\"namespace\":\"someone-else\"}",
                CancellationToken.None);
            await File.WriteAllTextAsync(
                Path.Combine(malformed, "metadata.json"),
                "{not-json",
                CancellationToken.None);
            await File.WriteAllTextAsync(
                Path.Combine(unmanaged, "must-survive.txt"),
                "sentinel",
                CancellationToken.None);

            Assert.Equal([managed], fixture.ManagedBackups());
            FixtureSnapshot snapshot = await fixture.SnapshotAsync(CancellationToken.None);
            Assert.Equal(1, snapshot.ManagedBackupCount);
        }
        finally
        {
            if (Directory.Exists(parent))
            {
                Directory.Delete(parent, recursive: true);
            }
        }
    }
}
