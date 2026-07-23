using System.Text;

namespace CodexProviderSync.App.Tests;

public sealed class ExecutionLogServiceTests
{
    [Fact]
    public void Append_WritesUtf8MultilineContentAndAllowsConcurrentReads()
    {
        string root = TempDirectory();
        DateTimeOffset now = new(2026, 7, 23, 10, 30, 0, TimeSpan.FromHours(8));
        try
        {
            ExecutionLogService service = new(root, () => now);

            Assert.True(service.TryAppend("第一行\n第二行", out Exception? firstError), firstError?.ToString());
            Assert.Equal(
                Path.Combine(root, "execution-2026-07-23.log"),
                service.CurrentLogPath);

            using FileStream reader = new(
                service.CurrentLogPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            Assert.True(service.TryAppend("继续写入", out Exception? secondError), secondError?.ToString());
            reader.Position = 0;
            using StreamReader textReader = new(reader, Encoding.UTF8);
            string content = textReader.ReadToEnd();

            Assert.Contains("第一行", content);
            Assert.Contains("第二行", content);
            Assert.Contains("继续写入", content);
            Assert.False(content.StartsWith("\uFEFF", StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void StartupPrune_DeletesOnlyExpiredDailyExecutionLogs()
    {
        string root = TempDirectory();
        DateTimeOffset now = new(2026, 7, 23, 9, 0, 0, TimeSpan.Zero);
        string expired = Path.Combine(root, "execution-2026-06-23.log");
        string oldestRetained = Path.Combine(root, "execution-2026-06-24.log");
        string recent = Path.Combine(root, "execution-2026-07-22.log");
        string unrelated = Path.Combine(root, "notes.log");
        string invalid = Path.Combine(root, "execution-latest.log");
        File.WriteAllText(expired, "expired");
        File.WriteAllText(oldestRetained, "keep");
        File.WriteAllText(recent, "keep");
        File.WriteAllText(unrelated, "keep");
        File.WriteAllText(invalid, "keep");

        try
        {
            _ = new ExecutionLogService(root, () => now, retentionDays: 30);

            Assert.False(File.Exists(expired));
            Assert.True(File.Exists(oldestRetained));
            Assert.True(File.Exists(recent));
            Assert.True(File.Exists(unrelated));
            Assert.True(File.Exists(invalid));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Append_WhenLogDirectoryIsUnavailable_ReturnsFailureWithoutThrowing()
    {
        string root = TempDirectory();
        string blockingFile = Path.Combine(root, "not-a-directory");
        File.WriteAllText(blockingFile, "block");
        try
        {
            ExecutionLogService service = new(blockingFile);

            bool written = service.TryAppend("test", out Exception? error);

            Assert.False(written);
            Assert.NotNull(error);
            Assert.True(File.Exists(blockingFile));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static string TempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"codex-provider-log-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }
}
