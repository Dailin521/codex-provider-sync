using System.Security.Cryptography;
using System.Text;

namespace CodexProviderSync.App.Tests;

public sealed class UpdateApplierTests
{
    [Fact]
    public void Apply_AtomicallyReplacesTargetAndRestartsUpdatedApplication()
    {
        FakeUpdateRuntime runtime = new();
        byte[] original = Encoding.UTF8.GetBytes("old executable");
        byte[] update = Encoding.UTF8.GetBytes("new executable");
        runtime.AddFile("download.exe", update);
        runtime.AddFile("installed.exe", original);
        UpdateApplyEngine engine = new(runtime);

        engine.Apply(new UpdateArguments(42, "download.exe", "installed.exe", Sha256(update)));

        Assert.Equal(update, runtime.ReadFile("installed.exe"));
        Assert.False(runtime.FileExists("download.exe"));
        Assert.False(runtime.FileExists("installed.exe.previous"));
        Assert.False(runtime.FileExists("installed.exe.update"));
        Assert.Equal(["installed.exe"], runtime.StartedProcesses);
        Assert.Equal([(42, TimeSpan.FromSeconds(30))], runtime.ProcessWaits);
        Assert.Equal(1, runtime.ReplaceCalls);
    }

    [Fact]
    public void Apply_WhenAtomicReplaceFails_PreservesOldTargetAndCanRestartIt()
    {
        FakeUpdateRuntime runtime = new() { ReplaceException = new IOException("replace failed") };
        byte[] original = Encoding.UTF8.GetBytes("old executable");
        byte[] update = Encoding.UTF8.GetBytes("new executable");
        runtime.AddFile("download.exe", update);
        runtime.AddFile("installed.exe", original);
        UpdateApplyEngine engine = new(runtime);
        UpdateArguments arguments = new(42, "download.exe", "installed.exe", Sha256(update));

        Assert.Throws<IOException>(() => engine.Apply(arguments));
        engine.TryRestartInstalledApplication(arguments.Target);

        Assert.Equal(original, runtime.ReadFile("installed.exe"));
        Assert.True(runtime.FileExists("download.exe"));
        Assert.False(runtime.FileExists("installed.exe.update"));
        Assert.Equal(["installed.exe"], runtime.StartedProcesses);
    }

    [Fact]
    public void Apply_WhenDownloadedFileWasModified_StopsBeforeReplacement()
    {
        FakeUpdateRuntime runtime = new();
        byte[] original = Encoding.UTF8.GetBytes("old executable");
        runtime.AddFile("download.exe", Encoding.UTF8.GetBytes("tampered executable"));
        runtime.AddFile("installed.exe", original);
        UpdateApplyEngine engine = new(runtime);

        Assert.Throws<InvalidDataException>(() =>
            engine.Apply(new UpdateArguments(42, "download.exe", "installed.exe", Sha256(Encoding.UTF8.GetBytes("expected executable")))));

        Assert.Equal(original, runtime.ReadFile("installed.exe"));
        Assert.Equal(0, runtime.ReplaceCalls);
        Assert.Empty(runtime.StartedProcesses);
    }

    [Fact]
    public void CleanupStaleUpdaterDirectories_RemovesOldHelpersAndKeepsCurrentOne()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-updater-cleanup-{Guid.NewGuid():N}");
        string stale = Path.Combine(root, "stale");
        string current = Path.Combine(root, "current");
        Directory.CreateDirectory(stale);
        Directory.CreateDirectory(current);
        File.WriteAllText(Path.Combine(stale, "CodexProviderSync.Updater.exe"), "old");
        File.WriteAllText(Path.Combine(current, "CodexProviderSync.Updater.exe"), "running");

        try
        {
            UpdateApplier.CleanupStaleUpdaterDirectories(root, current);

            Assert.False(Directory.Exists(stale));
            Assert.True(Directory.Exists(current));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void SystemRuntime_ReplaceFile_InstallsReplacementAndCreatesRollbackBackup()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-atomic-replace-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        string replacement = Path.Combine(root, "app.update");
        string target = Path.Combine(root, "app.exe");
        string backup = Path.Combine(root, "app.previous");
        File.WriteAllText(replacement, "new");
        File.WriteAllText(target, "old");

        try
        {
            new SystemUpdateRuntime().ReplaceFile(replacement, target, backup);

            Assert.Equal("new", File.ReadAllText(target));
            Assert.Equal("old", File.ReadAllText(backup));
            Assert.False(File.Exists(replacement));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string Sha256(byte[] content) => Convert.ToHexString(SHA256.HashData(content)).ToLowerInvariant();

    private sealed class FakeUpdateRuntime : IUpdateRuntime
    {
        private readonly Dictionary<string, byte[]> _files = new(StringComparer.OrdinalIgnoreCase);

        public Exception? ReplaceException { get; init; }
        public int ReplaceCalls { get; private set; }
        public List<string> StartedProcesses { get; } = [];
        public List<(int ProcessId, TimeSpan Timeout)> ProcessWaits { get; } = [];

        public void AddFile(string path, byte[] content) => _files[path] = content.ToArray();

        public byte[] ReadFile(string path) => _files[path].ToArray();

        public bool FileExists(string path) => _files.ContainsKey(path);

        public void CopyFile(string source, string destination, bool overwrite)
        {
            if (!overwrite && _files.ContainsKey(destination))
            {
                throw new IOException("destination exists");
            }

            _files[destination] = _files[source].ToArray();
        }

        public void ReplaceFile(string replacement, string target, string backup)
        {
            ReplaceCalls++;
            if (ReplaceException is not null)
            {
                throw ReplaceException;
            }

            _files[backup] = _files[target].ToArray();
            _files[target] = _files[replacement].ToArray();
            _files.Remove(replacement);
        }

        public void DeleteFile(string path) => _files.Remove(path);

        public byte[] CalculateSha256(string path) => SHA256.HashData(_files[path]);

        public void WaitForProcessExit(int processId, TimeSpan timeout) => ProcessWaits.Add((processId, timeout));

        public void StartProcess(string path) => StartedProcesses.Add(path);
    }
}
