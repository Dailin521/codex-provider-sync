using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core;

internal sealed class FileTransactionJournal
{
    internal const string FileName = "transaction-journal.jsonl";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly string _filePath;
    private readonly string _operationId;
    private int _sequence;

    private FileTransactionJournal(string filePath, string operationId, int sequence = 0)
    {
        _filePath = filePath;
        _operationId = operationId;
        _sequence = sequence;
    }

    internal static async Task<FileTransactionJournal> CreateAsync(
        string backupDir,
        string codexHome,
        string targetProvider,
        IEnumerable<string> potentialTargets)
    {
        string operationId = Guid.NewGuid().ToString("D");
        FileTransactionJournal journal = new(
            Path.Combine(backupDir, FileName),
            operationId);
        await journal.AppendAsync("prepared", new Dictionary<string, object?>
        {
            ["protocolVersion"] = 1,
            ["backupDir"] = Path.GetFullPath(backupDir),
            ["codexHome"] = Path.GetFullPath(codexHome),
            ["targetProvider"] = targetProvider,
            ["potentialTargets"] = potentialTargets
                .Select(Path.GetFullPath)
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal)
                .ToArray()
        });
        return journal;
    }

    internal Task ApplyingAsync(string kind, string targetPath) => AppendAsync(
        "applying",
        new Dictionary<string, object?>
        {
            ["kind"] = kind,
            ["targetPath"] = Path.GetFullPath(targetPath)
        });

    internal Task AppliedAsync(string kind, string targetPath) => AppendAsync(
        "applied",
        new Dictionary<string, object?>
        {
            ["kind"] = kind,
            ["targetPath"] = Path.GetFullPath(targetPath)
        });

    internal Task CommittedAsync() => AppendAsync("committed");

    internal Task RollingBackAsync(Exception originalError) => AppendAsync(
        "rollingBack",
        new Dictionary<string, object?> { ["originalError"] = originalError.Message });

    internal Task RolledBackAsync() => AppendAsync("rolledBack");

    internal Task RecoveryRequiredAsync(Exception originalError, IReadOnlyList<string> rollbackErrors) => AppendAsync(
        "recoveryRequired",
        new Dictionary<string, object?>
        {
            ["originalError"] = originalError.Message,
            ["rollbackErrors"] = rollbackErrors
        });

    private async Task AppendAsync(string state, IReadOnlyDictionary<string, object?>? details = null)
    {
        Dictionary<string, object?> value = new()
        {
            ["protocolVersion"] = 1,
            ["operationId"] = _operationId,
            ["sequence"] = ++_sequence,
            ["state"] = state,
            ["recordedAt"] = DateTimeOffset.UtcNow
        };
        if (details is not null)
        {
            foreach ((string key, object? detail) in details)
            {
                value[key] = detail;
            }
        }

        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, JsonOptions) + Environment.NewLine);
        await using FileStream stream = new(
            _filePath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            4096,
            FileOptions.Asynchronous | FileOptions.WriteThrough);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
        stream.Flush(flushToDisk: true);
    }

    internal static async Task<IReadOnlyList<PendingTransactionInfo>> FindPendingAsync(string codexHome)
    {
        string root = new CodexHomeService().BackupRoot(codexHome);
        if (!Directory.Exists(root))
        {
            return [];
        }

        List<PendingTransactionInfo> pending = [];
        foreach (string directory in Directory.EnumerateDirectories(root).Order(StringComparer.Ordinal))
        {
            string journalPath = Path.Combine(directory, FileName);
            if (!File.Exists(journalPath))
            {
                continue;
            }

            PendingTransactionInfo info = await ReadInfoAsync(journalPath);
            if (!info.Terminal)
            {
                pending.Add(info);
            }
        }
        return pending;
    }

    internal static async Task AssertNoPendingAsync(string codexHome)
    {
        IReadOnlyList<PendingTransactionInfo> pending = await FindPendingAsync(codexHome);
        if (pending.Count == 0)
        {
            return;
        }

        string backups = string.Join(", ", pending.Select(static item => item.BackupDir));
        throw new RecoveryRequiredException(
            $"An unfinished provider-sync transaction requires recovery before another write. Restore the bound backup, then retry. Backup(s): {backups}",
            pending);
    }

    internal static async Task MarkBackupRolledBackAsync(string backupDir)
    {
        string journalPath = Path.Combine(Path.GetFullPath(backupDir), FileName);
        if (!File.Exists(journalPath))
        {
            return;
        }

        PendingTransactionInfo info = await ReadInfoAsync(journalPath);
        if (info.Terminal)
        {
            return;
        }

        FileTransactionJournal journal = new(
            journalPath,
            info.OperationId ?? Guid.NewGuid().ToString("D"),
            info.LastSequence);
        await journal.RolledBackAsync();
    }

    private static async Task<PendingTransactionInfo> ReadInfoAsync(string journalPath)
    {
        string? operationId = null;
        int lastSequence = 0;
        string state = "recoveryRequired";
        bool invalidTail = false;
        foreach (string line in await File.ReadAllLinesAsync(journalPath))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            try
            {
                using JsonDocument document = JsonDocument.Parse(line);
                JsonElement root = document.RootElement;
                operationId ??= root.TryGetProperty("operationId", out JsonElement operation)
                    ? operation.GetString()
                    : null;
                lastSequence = root.TryGetProperty("sequence", out JsonElement sequence)
                    ? sequence.GetInt32()
                    : lastSequence;
                state = root.TryGetProperty("state", out JsonElement stateValue)
                    ? stateValue.GetString() ?? state
                    : state;
            }
            catch (JsonException)
            {
                invalidTail = true;
                state = "recoveryRequired";
                break;
            }
        }

        bool terminal = !invalidTail && state is "committed" or "rolledBack";
        return new PendingTransactionInfo(
            journalPath,
            Path.GetDirectoryName(journalPath)!,
            operationId,
            lastSequence,
            state,
            terminal,
            invalidTail);
    }
}

internal sealed record PendingTransactionInfo(
    string JournalPath,
    string BackupDir,
    string? OperationId,
    int LastSequence,
    string State,
    bool Terminal,
    bool InvalidTail);

public sealed class RecoveryRequiredException : InvalidOperationException
{
    internal RecoveryRequiredException(string message, IReadOnlyList<PendingTransactionInfo> pending)
        : base(message)
    {
        PendingBackupDirectories = pending.Select(static item => item.BackupDir).ToArray();
    }

    public string Code => "RECOVERY_REQUIRED";

    public IReadOnlyList<string> PendingBackupDirectories { get; }
}
