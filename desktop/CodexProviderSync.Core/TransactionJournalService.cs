using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core;

internal sealed class FileTransactionJournal
{
    internal const string FileName = "transaction-journal.jsonl";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly string _filePath;
    private readonly string _operationId;
    private readonly SemaphoreSlim _appendGate = new(1, 1);
    private int _sequence;

    private FileTransactionJournal(string filePath, string operationId, int sequence = 0)
    {
        _filePath = filePath;
        _operationId = operationId;
        _sequence = sequence;
    }

    internal string FilePath => _filePath;

    internal Func<string, string, Task>? AppendFaultInjector { get; set; }

    internal Task<PendingTransactionInfo> ReadCurrentInfoAsync() => ReadInfoAsync(_filePath);

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
                .Distinct(PathComparer)
                .Order(PathComparer)
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

    internal Task SkippedAsync(string kind, string targetPath) => AppendAsync(
        "skipped",
        new Dictionary<string, object?>
        {
            ["kind"] = kind,
            ["targetPath"] = Path.GetFullPath(targetPath)
        });

    internal Task CommittedAsync() => AppendTerminalAsync("committed");

    internal Task RollingBackAsync(Exception originalError) => AppendAsync(
        "rollingBack",
        new Dictionary<string, object?> { ["originalError"] = originalError.Message });

    internal Task RolledBackAsync() => AppendTerminalAsync("rolledBack");

    internal Task RecoveryRequiredAsync(Exception originalError, IReadOnlyList<string> rollbackErrors) => AppendAsync(
        "recoveryRequired",
        new Dictionary<string, object?>
        {
            ["originalError"] = originalError.Message,
            ["rollbackErrors"] = rollbackErrors
        });

    private async Task AppendAsync(string state, IReadOnlyDictionary<string, object?>? details = null)
    {
        await _appendGate.WaitAsync();
        try
        {
            PendingTransactionInfo? before = null;
            if (File.Exists(_filePath) && new FileInfo(_filePath).Length > 0)
            {
                before = await ReadInfoAsync(_filePath);
                if (before.InvalidTail)
                {
                    throw new InvalidOperationException(
                        state == "committed"
                            ? $"Transaction journal is invalid and cannot commit until recovery: {_filePath}"
                            : $"Transaction journal is invalid and requires recovery before append: {_filePath}");
                }
                if (!string.Equals(before.OperationId, _operationId, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Transaction journal operationId changed before append: {_filePath}");
                }
                _sequence = before.LastSequence;
            }
            else if (_sequence != 0)
            {
                throw new InvalidOperationException(
                    $"Transaction journal disappeared after it was created: {_filePath}");
            }

            ValidateAppendTransition(before, state, details);
            int nextSequence = _sequence + 1;
            Dictionary<string, object?> value = new()
            {
                ["protocolVersion"] = 1,
                ["operationId"] = _operationId,
                ["sequence"] = nextSequence,
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

            // The journal is a cross-runtime recovery protocol. Always use LF
            // so Node and .NET produce byte-compatible JSONL on every platform.
            byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, JsonOptions) + "\n");
            try
            {
                if (AppendFaultInjector is not null)
                {
                    await AppendFaultInjector("before-write", state);
                }
                await using (FileStream stream = new(
                    _filePath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.Read,
                    4096,
                    FileOptions.Asynchronous | FileOptions.WriteThrough))
                {
                    await stream.WriteAsync(bytes);
                    if (AppendFaultInjector is not null)
                    {
                        await AppendFaultInjector("after-write-before-flush", state);
                    }
                    await stream.FlushAsync();
                    stream.Flush(flushToDisk: true);
                }
                if (AppendFaultInjector is not null)
                {
                    await AppendFaultInjector("after-flush-before-verify", state);
                }
            }
            catch
            {
                await ReconcileSequenceAfterAppendAttemptAsync();
                throw;
            }

            PendingTransactionInfo after = await ReadInfoAsync(_filePath);
            _sequence = after.LastSequence;
            if (after.InvalidTail
                || !string.Equals(after.OperationId, _operationId, StringComparison.Ordinal)
                || after.LastSequence != nextSequence
                || !string.Equals(after.State, state, StringComparison.Ordinal)
                || (state is "committed" or "rolledBack") != after.Terminal)
            {
                throw new InvalidOperationException(
                    $"Transaction journal append could not be verified after writing {state}: {_filePath}");
            }
        }
        finally
        {
            _appendGate.Release();
        }
    }

    private async Task AppendTerminalAsync(string state)
    {
        try
        {
            await AppendAsync(state);
        }
        catch
        {
            // A flush API may report failure after the complete terminal record
            // reached the durable/readable journal. Treat that exact terminal
            // event as authoritative so the coordinator never compensates a
            // transaction after it was already recorded committed.
            PendingTransactionInfo current = await ReadInfoAsync(_filePath);
            if (!current.InvalidTail
                && current.Terminal
                && string.Equals(current.OperationId, _operationId, StringComparison.Ordinal)
                && string.Equals(current.State, state, StringComparison.Ordinal))
            {
                _sequence = current.LastSequence;
                return;
            }
            throw;
        }
    }

    private async Task ReconcileSequenceAfterAppendAttemptAsync()
    {
        if (!File.Exists(_filePath))
        {
            _sequence = 0;
            return;
        }
        try
        {
            PendingTransactionInfo current = await ReadInfoAsync(_filePath);
            if (string.Equals(current.OperationId, _operationId, StringComparison.Ordinal))
            {
                _sequence = current.LastSequence;
            }
        }
        catch
        {
            // Preserve the original append failure. The next append performs a
            // full journal validation and will fail closed if recovery is needed.
        }
    }

    private static void ValidateAppendTransition(
        PendingTransactionInfo? current,
        string nextState,
        IReadOnlyDictionary<string, object?>? details)
    {
        if (current is null)
        {
            if (!string.Equals(nextState, "prepared", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("A transaction journal must begin with prepared.");
            }
            return;
        }

        if (current.Terminal || current.State is "committed" or "rolledBack")
        {
            throw new InvalidOperationException(
                $"Transaction journal is already terminal ({current.State}) and cannot append {nextState}: {current.JournalPath}");
        }
        if (string.Equals(nextState, "prepared", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("A transaction journal cannot contain a second prepared record.");
        }
        if (current.State == "rollingBack" && nextState is not ("rolledBack" or "recoveryRequired"))
        {
            throw new InvalidOperationException(
                $"Transaction journal cannot append {nextState} after rollingBack.");
        }
        if (current.State == "recoveryRequired" && nextState != "rolledBack")
        {
            throw new InvalidOperationException(
                $"Transaction journal cannot append {nextState} after recoveryRequired.");
        }
        if (nextState == "rolledBack" && current.State is not ("rollingBack" or "recoveryRequired"))
        {
            throw new InvalidOperationException(
                $"Transaction journal cannot append rolledBack after {current.State}.");
        }
        if (nextState == "committed")
        {
            if (current.State is "rollingBack" or "recoveryRequired"
                || current.AffectedTargets.Any(static target => target.State == "applying"))
            {
                throw new InvalidOperationException(
                    $"Transaction journal cannot commit while rollback or target application is unresolved: {current.JournalPath}");
            }
            return;
        }
        if (nextState is "rollingBack" or "recoveryRequired" or "rolledBack")
        {
            return;
        }
        if (nextState is not ("applying" or "applied" or "skipped"))
        {
            throw new InvalidOperationException($"Unknown transaction journal state: {nextState}");
        }
        if (current.State is "rollingBack" or "recoveryRequired")
        {
            throw new InvalidOperationException(
                $"Transaction journal cannot mutate targets after {current.State}.");
        }

        string kind = ReadRequiredDetail(details, "kind");
        string targetPathValue = ReadRequiredDetail(details, "targetPath");
        if (kind is not ("config" or "rollout" or "globalState" or "sqlite")
            || !Path.IsPathFullyQualified(targetPathValue))
        {
            throw new InvalidOperationException("Transaction target details are invalid.");
        }
        string targetPath = Path.GetFullPath(targetPathValue);
        if (!current.PotentialTargets.Contains(targetPath, PathComparer))
        {
            throw new InvalidOperationException(
                $"Transaction target was not declared by prepared: {targetPath}");
        }
        string keyKind = kind;
        TransactionTargetInfo? affected = current.AffectedTargets.FirstOrDefault(target =>
            string.Equals(target.Kind, keyKind, StringComparison.Ordinal)
            && PathComparer.Equals(target.TargetPath, targetPath));
        if (nextState == "applying" && affected is not null)
        {
            throw new InvalidOperationException(
                $"Transaction target is already being tracked: {targetPath}");
        }
        if (nextState is "applied" or "skipped"
            && affected?.State != "applying")
        {
            throw new InvalidOperationException(
                $"Transaction target must be applying before {nextState}: {targetPath}");
        }
    }

    private static string ReadRequiredDetail(
        IReadOnlyDictionary<string, object?>? details,
        string name)
    {
        return details is not null
            && details.TryGetValue(name, out object? value)
            && value is string text
            && !string.IsNullOrWhiteSpace(text)
                ? text
                : throw new InvalidOperationException($"Transaction journal detail {name} is required.");
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

    internal static async Task MarkBackupRolledBackAsync(
        string backupDir,
        string codexHome,
        string targetProvider)
    {
        string journalPath = Path.Combine(Path.GetFullPath(backupDir), FileName);
        if (!File.Exists(journalPath))
        {
            return;
        }

        JournalReadResult readResult = await ReadJournalAsync(journalPath);
        PendingTransactionInfo info = readResult.Info;
        if (info.Terminal)
        {
            return;
        }

        if (info.InvalidTail)
        {
            string invalidArchivePath = Path.Combine(
                Path.GetDirectoryName(journalPath)!,
                $"transaction-journal.invalid.{DateTimeOffset.UtcNow:yyyyMMdd'T'HHmmssfff'Z'}.{Guid.NewGuid():N}.jsonl");
            await AtomicFile.CopyAsync(journalPath, invalidArchivePath, overwrite: false);

            if (string.IsNullOrWhiteSpace(info.OperationId) || readResult.ValidLines.Count == 0)
            {
                await AtomicFile.WriteAllTextAsync(journalPath, string.Empty);
                FileTransactionJournal replacement = await CreateAsync(
                    Path.GetDirectoryName(journalPath)!,
                    codexHome,
                    targetProvider,
                    []);
                await replacement.RollingBackAsync(
                    new InvalidOperationException("Explicit managed-backup restore repaired an unreadable journal"));
                await replacement.RolledBackAsync();
                await AssertRolledBackTerminalAsync(replacement);
                return;
            }

            List<string> validPrefix = [.. readResult.ValidLines];
            if (info.LastValidState is "committed" or "rolledBack")
            {
                validPrefix.RemoveAt(validPrefix.Count - 1);
            }
            string normalizedPrefix = string.Join("\n", validPrefix) + "\n";
            await AtomicFile.WriteAllTextAsync(journalPath, normalizedPrefix);
            info = await ReadInfoAsync(journalPath);
        }

        FileTransactionJournal journal = new(
            journalPath,
            info.OperationId!,
            info.LastSequence);
        if (info.State is not ("rollingBack" or "recoveryRequired"))
        {
            await journal.RollingBackAsync(new InvalidOperationException("Explicit managed-backup restore"));
        }
        await journal.RolledBackAsync();
        await AssertRolledBackTerminalAsync(journal);
    }

    private static async Task AssertRolledBackTerminalAsync(FileTransactionJournal journal)
    {
        PendingTransactionInfo verified = await journal.ReadCurrentInfoAsync();
        if (verified.InvalidTail
            || !verified.Terminal
            || !string.Equals(verified.State, "rolledBack", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Transaction journal repair did not reach a verified rolledBack state: {journal.FilePath}");
        }
    }

    internal static async Task<PendingTransactionInfo> ReadInfoAsync(string journalPath)
    {
        return (await ReadJournalAsync(journalPath)).Info;
    }

    private static async Task<JournalReadResult> ReadJournalAsync(string journalPath)
    {
        byte[] journalBytes = await File.ReadAllBytesAsync(journalPath);
        bool missingTerminalLf = journalBytes.Length > 0 && journalBytes[^1] != (byte)'\n';
        string journalText = Encoding.UTF8.GetString(journalBytes);
        string? operationId = null;
        int lastSequence = 0;
        string state = "recoveryRequired";
        string lastValidState = "none";
        bool invalidTail = false;
        bool sawRecord = false;
        bool sawTerminal = false;
        List<string> validLines = [];
        List<string> potentialTargets = [];
        Dictionary<string, TransactionTargetInfo> affectedTargets = new(PathComparer);
        foreach (string line in journalText.Split('\n'))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            try
            {
                using JsonDocument document = JsonDocument.Parse(line);
                JsonElement root = document.RootElement;
                string? recordOperationId = root.TryGetProperty("operationId", out JsonElement operation)
                    ? operation.GetString()
                    : null;
                int recordSequence = root.TryGetProperty("sequence", out JsonElement sequence)
                    && sequence.TryGetInt32(out int parsedSequence)
                        ? parsedSequence
                        : -1;
                string? recordState = root.TryGetProperty("state", out JsonElement stateValue)
                    ? stateValue.GetString()
                    : null;
                int protocolVersion = root.TryGetProperty("protocolVersion", out JsonElement protocol)
                    && protocol.TryGetInt32(out int parsedProtocol)
                        ? parsedProtocol
                        : -1;

                if (protocolVersion != 1
                    || string.IsNullOrWhiteSpace(recordOperationId)
                    || !Guid.TryParse(recordOperationId, out _)
                    || recordSequence != lastSequence + 1
                    || string.IsNullOrWhiteSpace(recordState)
                    || sawTerminal
                    || (operationId is not null
                        && !string.Equals(operationId, recordOperationId, StringComparison.Ordinal)))
                {
                    invalidTail = true;
                    state = "recoveryRequired";
                    break;
                }

                if (sawRecord
                    && (recordState == "prepared"
                        || (state == "rollingBack" && recordState is not ("rolledBack" or "recoveryRequired"))
                        || (state == "recoveryRequired" && recordState != "rolledBack")
                        || (recordState == "rolledBack" && state is not ("rollingBack" or "recoveryRequired"))))
                {
                    invalidTail = true;
                    state = "recoveryRequired";
                    break;
                }

                if (!sawRecord)
                {
                    if (!string.Equals(recordState, "prepared", StringComparison.Ordinal)
                        || recordSequence != 1)
                    {
                        invalidTail = true;
                        state = "recoveryRequired";
                        break;
                    }

                    operationId = recordOperationId;
                    if (!root.TryGetProperty("potentialTargets", out JsonElement targets)
                        || targets.ValueKind != JsonValueKind.Array)
                    {
                        throw new InvalidOperationException("Prepared journal record must declare potentialTargets.");
                    }

                    HashSet<string> declaredTargets = new(PathComparer);
                    foreach (JsonElement target in targets.EnumerateArray())
                    {
                        string? targetPath = target.GetString();
                        if (string.IsNullOrWhiteSpace(targetPath)
                            || !Path.IsPathFullyQualified(targetPath))
                        {
                            throw new InvalidOperationException(
                                "Prepared journal potentialTargets must contain absolute paths.");
                        }
                        string fullTargetPath = Path.GetFullPath(targetPath);
                        if (!declaredTargets.Add(fullTargetPath))
                        {
                            throw new InvalidOperationException(
                                $"Prepared journal contains a duplicate potential target: {fullTargetPath}");
                        }
                        potentialTargets.Add(fullTargetPath);
                    }
                }
                else if (recordState is "applying" or "applied" or "skipped")
                {
                    string? kind = root.TryGetProperty("kind", out JsonElement kindValue)
                        ? kindValue.GetString()
                        : null;
                    string? targetPath = root.TryGetProperty("targetPath", out JsonElement targetValue)
                        ? targetValue.GetString()
                        : null;
                    if (kind is not ("config" or "rollout" or "globalState" or "sqlite")
                        || string.IsNullOrWhiteSpace(targetPath)
                        || !Path.IsPathFullyQualified(targetPath))
                    {
                        invalidTail = true;
                        state = "recoveryRequired";
                        break;
                    }

                    string fullTargetPath = Path.GetFullPath(targetPath);
                    if (!potentialTargets.Contains(fullTargetPath, PathComparer))
                    {
                        invalidTail = true;
                        state = "recoveryRequired";
                        break;
                    }

                    string key = kind + "\0" + fullTargetPath;
                    if (recordState is "applied" or "skipped"
                        && (!affectedTargets.TryGetValue(key, out TransactionTargetInfo? applying)
                            || applying.State != "applying"))
                    {
                        invalidTail = true;
                        state = "recoveryRequired";
                        break;
                    }

                    if (recordState == "skipped")
                    {
                        affectedTargets.Remove(key);
                    }
                    else
                    {
                        affectedTargets[key] = new TransactionTargetInfo(kind, fullTargetPath, recordState);
                    }
                }
                else if (recordState is not (
                    "prepared" or "committed" or "rollingBack" or "rolledBack" or "recoveryRequired"))
                {
                    invalidTail = true;
                    state = "recoveryRequired";
                    break;
                }

                if (recordState == "committed"
                    && affectedTargets.Values.Any(static target => target.State == "applying"))
                {
                    invalidTail = true;
                    state = "recoveryRequired";
                    break;
                }
                sawRecord = true;
                lastSequence = recordSequence;
                state = recordState;
                lastValidState = recordState;
                sawTerminal = recordState is "committed" or "rolledBack";
                validLines.Add(line);
            }
            catch (Exception error) when (error is JsonException or InvalidOperationException or ArgumentException or NotSupportedException)
            {
                invalidTail = true;
                state = "recoveryRequired";
                break;
            }
        }

        if (!sawRecord)
        {
            invalidTail = true;
            state = "recoveryRequired";
        }
        else if (missingTerminalLf)
        {
            invalidTail = true;
            state = "recoveryRequired";
        }

        bool terminal = !invalidTail && state is "committed" or "rolledBack";
        PendingTransactionInfo info = new(
            journalPath,
            Path.GetDirectoryName(journalPath)!,
            operationId,
            lastSequence,
            state,
            terminal,
            invalidTail,
            lastValidState,
            potentialTargets.Distinct(PathComparer).Order(PathComparer).ToArray(),
            affectedTargets.Values.ToArray());
        return new JournalReadResult(info, validLines);
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    private sealed record JournalReadResult(PendingTransactionInfo Info, IReadOnlyList<string> ValidLines);
}

internal sealed record TransactionTargetInfo(string Kind, string TargetPath, string State);

internal sealed record PendingTransactionInfo(
    string JournalPath,
    string BackupDir,
    string? OperationId,
    int LastSequence,
    string State,
    bool Terminal,
    bool InvalidTail,
    string LastValidState,
    IReadOnlyList<string> PotentialTargets,
    IReadOnlyList<TransactionTargetInfo> AffectedTargets);

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
