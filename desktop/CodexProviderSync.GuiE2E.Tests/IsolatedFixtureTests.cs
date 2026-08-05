namespace CodexProviderSync.GuiE2E.Tests;

public sealed class IsolatedFixtureTests
{
    [Fact]
    public async Task Snapshot_ReleasesEveryExistingSqliteStorageFileForMove()
    {
        string parent = Path.Combine(
            Path.GetTempPath(),
            "codex-provider-sync-gui-e2e-tests",
            Guid.NewGuid().ToString("N"));
        string root = Path.Combine(parent, "isolation");
        Directory.CreateDirectory(parent);
        try
        {
            IsolatedFixture fixture = new(root);
            await fixture.InitializeAsync(CancellationToken.None);

            _ = await fixture.SnapshotAsync(CancellationToken.None);

            string[] existing = new[]
            {
                fixture.DatabasePath,
                fixture.DatabasePath + "-wal",
                fixture.DatabasePath + "-shm"
            }.Where(File.Exists).ToArray();
            Assert.Contains(fixture.DatabasePath, existing, StringComparer.OrdinalIgnoreCase);
            foreach (string source in existing)
            {
                string moved = source + ".move-proof";
                File.Move(source, moved, overwrite: false);
                Assert.False(File.Exists(source));
                File.Move(moved, source, overwrite: false);
                Assert.True(File.Exists(source));
            }
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
