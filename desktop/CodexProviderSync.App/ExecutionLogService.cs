using System.Globalization;
using System.Text;
using CodexProviderSync.Core;

namespace CodexProviderSync.App;

internal sealed class ExecutionLogService
{
    internal const int DefaultRetentionDays = 30;
    private const string FilePrefix = "execution-";
    private const string FileSuffix = ".log";

    private readonly object _gate = new();
    private readonly Func<DateTimeOffset> _clock;
    private readonly int _retentionDays;
    private DateOnly? _lastPrunedDate;

    public ExecutionLogService(
        string? logDirectory = null,
        Func<DateTimeOffset>? clock = null,
        int retentionDays = DefaultRetentionDays)
    {
        if (retentionDays < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionDays), retentionDays, "日志保留天数必须大于 0。");
        }

        LogDirectory = string.IsNullOrWhiteSpace(logDirectory)
            ? Path.Combine(AppConstants.SettingsDirectory(), "logs")
            : Path.GetFullPath(logDirectory);
        _clock = clock ?? (() => DateTimeOffset.Now);
        _retentionDays = retentionDays;
        TryPruneOldLogs(out _);
    }

    public string LogDirectory { get; }

    public string CurrentLogPath
    {
        get
        {
            DateOnly date = DateOnly.FromDateTime(_clock().LocalDateTime);
            return Path.Combine(LogDirectory, $"{FilePrefix}{date:yyyy-MM-dd}{FileSuffix}");
        }
    }

    public bool TryAppend(string message, out Exception? error)
    {
        lock (_gate)
        {
            try
            {
                DateOnly today = DateOnly.FromDateTime(_clock().LocalDateTime);
                if (_lastPrunedDate != today)
                {
                    TryPruneOldLogs(out _);
                }

                Directory.CreateDirectory(LogDirectory);
                using FileStream stream = new(
                    CurrentLogPath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.ReadWrite | FileShare.Delete);
                using StreamWriter writer = new(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                writer.WriteLine(message);
                error = null;
                return true;
            }
            catch (Exception caught)
            {
                error = caught;
                return false;
            }
        }
    }

    internal bool TryPruneOldLogs(out Exception? error)
    {
        lock (_gate)
        {
            try
            {
                DateOnly today = DateOnly.FromDateTime(_clock().LocalDateTime);
                DateOnly oldestRetainedDate = today.AddDays(-(_retentionDays - 1));
                if (!Directory.Exists(LogDirectory))
                {
                    _lastPrunedDate = today;
                    error = null;
                    return true;
                }

                foreach (string path in Directory.EnumerateFiles(LogDirectory, $"{FilePrefix}*{FileSuffix}"))
                {
                    string name = Path.GetFileName(path);
                    if (!TryParseLogDate(name, out DateOnly logDate) || logDate >= oldestRetainedDate)
                    {
                        continue;
                    }

                    File.Delete(path);
                }

                _lastPrunedDate = today;
                error = null;
                return true;
            }
            catch (Exception caught)
            {
                error = caught;
                return false;
            }
        }
    }

    private static bool TryParseLogDate(string fileName, out DateOnly date)
    {
        date = default;
        if (!fileName.StartsWith(FilePrefix, StringComparison.Ordinal)
            || !fileName.EndsWith(FileSuffix, StringComparison.Ordinal))
        {
            return false;
        }

        string value = fileName[FilePrefix.Length..^FileSuffix.Length];
        return DateOnly.TryParseExact(
            value,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out date);
    }
}
