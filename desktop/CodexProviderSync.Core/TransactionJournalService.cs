using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Core;

internal sealed class FileTransactionJournal : IAsyncDisposable
{
    internal const string FileName = "transaction-journal.jsonl";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly string _filePath;
    private readonly string _operationId;
    private readonly SemaphoreSlim _appendGate = new(1, 1);
    private FileStream? _writerLease;
    private int _sequence;
    private PendingTransactionInfo? _current;
    private long _expectedLength;
    private byte[] _expectedTailRecord = [];
    private bool _disposed;

    private FileTransactionJournal(
        string filePath,
        string operationId,
        PendingTransactionInfo? current = null,
        FileStream? writerLease = null)
    {
        _filePath = filePath;
        _operationId = operationId;
        _current = current;
        _writerLease = writerLease;
        _sequence = current?.LastSequence ?? 0;
        _expectedLength = current is null ? 0 : -1;
    }

    internal string FilePath => _filePath;

    internal int AppendFullJournalValidationCount { get; private set; }

    internal Func<string, string, Task>? AppendFaultInjector { get; set; }

    internal async Task<PendingTransactionInfo> ReadCurrentInfoAsync()
    {
        await _appendGate.WaitAsync();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return (await ReadJournalForCurrentInstanceAsync()).Info;
        }
        finally
        {
            _appendGate.Release();
        }
    }

    internal static async Task<FileTransactionJournal> CreateAsync(
        string backupDir,
        string codexHome,
        string targetProvider,
        IEnumerable<string> potentialTargets)
    {
        return await CreateCoreAsync(
            backupDir,
            codexHome,
            targetProvider,
            potentialTargets,
            acquireWriterLease: false);
    }

    internal static async Task<FileTransactionJournal> CreateOwnedAsync(
        string backupDir,
        string codexHome,
        string targetProvider,
        IEnumerable<string> potentialTargets)
    {
        return await CreateCoreAsync(
            backupDir,
            codexHome,
            targetProvider,
            potentialTargets,
            acquireWriterLease: OperatingSystem.IsWindows());
    }

    private static async Task<FileTransactionJournal> CreateCoreAsync(
        string backupDir,
        string codexHome,
        string targetProvider,
        IEnumerable<string> potentialTargets,
        bool acquireWriterLease)
    {
        string filePath = Path.Combine(backupDir, FileName);
        string operationId = Guid.NewGuid().ToString("D");
        FileStream? writerLease = acquireWriterLease
            ? OpenWriterLease(filePath, FileMode.CreateNew)
            : null;
        FileTransactionJournal journal = new(filePath, operationId, writerLease: writerLease);
        try
        {
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
        catch
        {
            await journal.DisposeAsync();
            throw;
        }
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
            ObjectDisposedException.ThrowIf(_disposed, this);
            bool terminal = state is "committed" or "rolledBack";
            PendingTransactionInfo? before = await EnsureJournalFrontierAsync(forceFullValidation: terminal);

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
            long appendOffset = _expectedLength;
            try
            {
                if (AppendFaultInjector is not null)
                {
                    await AppendFaultInjector("before-write", state);
                }
                if (_writerLease is not null)
                {
                    if (_writerLease.Length != appendOffset)
                    {
                        throw new InvalidOperationException(
                            $"Transaction journal changed while preparing to append: {_filePath}");
                    }
                    _writerLease.Seek(appendOffset, SeekOrigin.Begin);
                    await _writerLease.WriteAsync(bytes);
                    if (AppendFaultInjector is not null)
                    {
                        await AppendFaultInjector("after-write-before-flush", state);
                    }
                    await _writerLease.FlushAsync();
                    _writerLease.Flush(flushToDisk: true);
                }
                else
                {
                    await using FileStream stream = new(
                        _filePath,
                        FileMode.Append,
                        FileAccess.Write,
                        FileShare.Read,
                        4096,
                        FileOptions.Asynchronous | FileOptions.WriteThrough);
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

                try
                {
                    await VerifyAppendedRecordAsync(appendOffset, bytes);
                }
                catch (InvalidOperationException error) when (terminal)
                {
                    throw new InvalidOperationException(
                        $"Transaction journal append could not be verified after writing {state}: {_filePath}",
                        error);
                }

                if (terminal)
                {
                    JournalReadResult verified = await ReadJournalForAppendAsync();
                    AdoptJournalState(verified);
                    PendingTransactionInfo afterTerminal = verified.Info;
                    if (afterTerminal.InvalidTail
                        || !afterTerminal.Terminal
                        || !string.Equals(afterTerminal.OperationId, _operationId, StringComparison.Ordinal)
                        || afterTerminal.LastSequence != nextSequence
                        || !string.Equals(afterTerminal.State, state, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            $"Transaction journal append could not be verified after writing {state}: {_filePath}");
                    }
                    return;
                }

                PendingTransactionInfo after = AdvanceJournalState(before, state, details, nextSequence);
                _current = after;
                _sequence = nextSequence;
                _expectedLength = appendOffset + bytes.Length;
                _expectedTailRecord = bytes;
            }
            catch
            {
                await ReconcileSequenceAfterAppendAttemptAsync();
                throw;
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
            JournalReadResult reconciled = await ReadJournalForCurrentInstanceAsync();
            PendingTransactionInfo current = reconciled.Info;
            if (!current.InvalidTail
                && current.Terminal
                && string.Equals(current.OperationId, _operationId, StringComparison.Ordinal)
                && string.Equals(current.State, state, StringComparison.Ordinal))
            {
                AdoptJournalState(reconciled);
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
            _current = null;
            _expectedLength = 0;
            _expectedTailRecord = [];
            return;
        }
        try
        {
            JournalReadResult read = await ReadJournalForAppendAsync();
            if (string.Equals(read.Info.OperationId, _operationId, StringComparison.Ordinal))
            {
                AdoptJournalState(read);
            }
            else
            {
                _expectedLength = -1;
                _expectedTailRecord = [];
            }
        }
        catch
        {
            // Preserve the original append failure. The next append performs a
            // full journal validation and will fail closed if recovery is needed.
        }
    }

    private async Task<PendingTransactionInfo?> EnsureJournalFrontierAsync(bool forceFullValidation)
    {
        if (!File.Exists(_filePath))
        {
            if (_sequence != 0 || _current is not null)
            {
                throw new InvalidOperationException(
                    $"Transaction journal disappeared after it was created: {_filePath}");
            }
            _expectedLength = 0;
            _expectedTailRecord = [];
            return null;
        }

        long actualLength = new FileInfo(_filePath).Length;
        if (actualLength == 0 && _sequence == 0 && _current is null)
        {
            _expectedLength = 0;
            _expectedTailRecord = [];
            return null;
        }
        bool frontierMatches = _writerLease is not null
            && OperatingSystem.IsWindows()
            && !forceFullValidation
            && actualLength == _expectedLength
            && await TailMatchesAsync(actualLength, _expectedTailRecord);
        if (!frontierMatches)
        {
            JournalReadResult read = await ReadJournalForAppendAsync();
            AdoptJournalState(read);
        }

        if (_current is null)
        {
            return null;
        }
        if (_current.InvalidTail)
        {
            throw new InvalidOperationException(
                forceFullValidation
                    ? $"Transaction journal is invalid and cannot commit until recovery: {_filePath}"
                    : $"Transaction journal is invalid and requires recovery before append: {_filePath}");
        }
        if (!string.Equals(_current.OperationId, _operationId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Transaction journal operationId changed before append: {_filePath}");
        }
        _sequence = _current.LastSequence;
        return _current;
    }

    private async Task<JournalReadResult> ReadJournalForAppendAsync()
    {
        AppendFullJournalValidationCount += 1;
        return await ReadJournalForCurrentInstanceAsync();
    }

    private Task<JournalReadResult> ReadJournalForCurrentInstanceAsync()
    {
        return _writerLease is null
            ? ReadJournalAsync(_filePath)
            : ReadJournalAsync(_filePath, _writerLease);
    }

    private void AdoptJournalState(JournalReadResult read)
    {
        _current = read.Info;
        _sequence = read.Info.LastSequence;
        _expectedLength = _writerLease?.Length ?? new FileInfo(_filePath).Length;
        _expectedTailRecord = read.ValidLines.Count == 0
            ? []
            : Encoding.UTF8.GetBytes(read.ValidLines[^1] + "\n");
    }

    private async Task VerifyAppendedRecordAsync(long appendOffset, byte[] expectedRecord)
    {
        if (_writerLease is not null)
        {
            long leasedExpectedLength = appendOffset + expectedRecord.Length;
            if (_writerLease.Length != leasedExpectedLength)
            {
                throw new InvalidOperationException(
                    $"Transaction journal changed while appending: {_filePath}");
            }
            _writerLease.Seek(appendOffset, SeekOrigin.Begin);
            byte[] leasedActual = new byte[expectedRecord.Length];
            await _writerLease.ReadExactlyAsync(leasedActual);
            if (!leasedActual.AsSpan().SequenceEqual(expectedRecord))
            {
                throw new InvalidOperationException(
                    $"Transaction journal append bytes could not be verified: {_filePath}");
            }
            return;
        }

        await using FileStream stream = new(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        long expectedLength = appendOffset + expectedRecord.Length;
        if (stream.Length != expectedLength)
        {
            throw new InvalidOperationException(
                $"Transaction journal changed while appending: {_filePath}");
        }

        stream.Seek(appendOffset, SeekOrigin.Begin);
        byte[] actual = new byte[expectedRecord.Length];
        await stream.ReadExactlyAsync(actual);
        if (!actual.AsSpan().SequenceEqual(expectedRecord))
        {
            throw new InvalidOperationException(
                $"Transaction journal append bytes could not be verified: {_filePath}");
        }
    }

    private async Task<bool> TailMatchesAsync(long actualLength, byte[] expectedTail)
    {
        if (actualLength == 0)
        {
            return expectedTail.Length == 0;
        }
        if (expectedTail.Length == 0 || actualLength < expectedTail.Length)
        {
            return false;
        }

        if (_writerLease is not null)
        {
            if (_writerLease.Length != actualLength)
            {
                return false;
            }
            _writerLease.Seek(actualLength - expectedTail.Length, SeekOrigin.Begin);
            byte[] leasedTail = new byte[expectedTail.Length];
            await _writerLease.ReadExactlyAsync(leasedTail);
            return leasedTail.AsSpan().SequenceEqual(expectedTail);
        }

        await using FileStream stream = new(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        if (stream.Length != actualLength)
        {
            return false;
        }
        stream.Seek(actualLength - expectedTail.Length, SeekOrigin.Begin);
        byte[] actualTail = new byte[expectedTail.Length];
        await stream.ReadExactlyAsync(actualTail);
        return actualTail.AsSpan().SequenceEqual(expectedTail);
    }

    public async ValueTask DisposeAsync()
    {
        await _appendGate.WaitAsync();
        try
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            FileStream? writerLease = Interlocked.Exchange(ref _writerLease, null);
            if (writerLease is not null)
            {
                await writerLease.DisposeAsync();
            }
        }
        finally
        {
            _appendGate.Release();
        }
    }

    private static FileStream OpenWriterLease(string filePath, FileMode mode)
    {
        return new FileStream(
            filePath,
            mode,
            FileAccess.ReadWrite,
            FileShare.Read,
            4096,
            FileOptions.Asynchronous | FileOptions.WriteThrough | FileOptions.SequentialScan);
    }

    private PendingTransactionInfo AdvanceJournalState(
        PendingTransactionInfo? current,
        string nextState,
        IReadOnlyDictionary<string, object?>? details,
        int nextSequence)
    {
        IReadOnlyList<string> potentialTargets = current?.PotentialTargets
            ?? ReadPreparedPotentialTargets(details);
        Dictionary<string, TransactionTargetInfo> affected = new(PathComparer);
        if (current is not null)
        {
            foreach (TransactionTargetInfo target in current.AffectedTargets)
            {
                affected[target.Kind + "\0" + Path.GetFullPath(target.TargetPath)] = target;
            }
        }

        if (nextState is "applying" or "applied" or "skipped")
        {
            string kind = ReadRequiredDetail(details, "kind");
            string targetPath = Path.GetFullPath(ReadRequiredDetail(details, "targetPath"));
            string key = kind + "\0" + targetPath;
            if (nextState == "skipped")
            {
                affected.Remove(key);
            }
            else
            {
                affected[key] = new TransactionTargetInfo(kind, targetPath, nextState);
            }
        }

        bool terminal = nextState is "committed" or "rolledBack";
        string journalPath = current?.JournalPath ?? _filePath;
        string? declaredBackupDir = current?.DeclaredBackupDir;
        if (declaredBackupDir is null && nextState == "prepared")
        {
            string declared = ReadRequiredDetail(details, "backupDir");
            declaredBackupDir = Path.IsPathFullyQualified(declared)
                ? Path.GetFullPath(declared)
                : throw new InvalidOperationException(
                    "Transaction journal detail backupDir must be an absolute path.");
        }
        return new PendingTransactionInfo(
            journalPath,
            current?.BackupDir ?? Path.GetDirectoryName(_filePath)!,
            declaredBackupDir,
            current?.OperationId ?? _operationId,
            nextSequence,
            nextState,
            terminal,
            InvalidTail: false,
            LastValidState: nextState,
            potentialTargets,
            affected.Values.ToArray());
    }

    private static IReadOnlyList<string> ReadPreparedPotentialTargets(
        IReadOnlyDictionary<string, object?>? details)
    {
        if (details is not null
            && details.TryGetValue("potentialTargets", out object? value)
            && value is IEnumerable<string> targets)
        {
            return targets
                .Select(Path.GetFullPath)
                .Distinct(PathComparer)
                .Order(PathComparer)
                .ToArray();
        }
        return [];
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
            string declaredBackupDir = ReadRequiredDetail(details, "backupDir");
            if (!Path.IsPathFullyQualified(declaredBackupDir))
            {
                throw new InvalidOperationException(
                    "Transaction journal detail backupDir must be an absolute path.");
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
        IReadOnlyList<RestoreJournalInfo> pendingRestores =
            await RestoreJournalService.FindBlockingAsync(codexHome);
        if (pending.Count == 0 && pendingRestores.Count == 0)
        {
            return;
        }

        string[] pendingDirectories = pending
            .Select(static item => item.BackupDir)
            .Concat(pendingRestores.Select(static item => item.SnapshotDir))
            .Select(Path.GetFullPath)
            .Distinct(PathComparer)
            .ToArray();
        string backups = string.Join(", ", pendingDirectories);
        throw new RecoveryRequiredException(
            $"An unfinished provider-sync transaction requires recovery before another write. Restore the bound backup, then retry. Backup(s): {backups}",
            pendingDirectories);
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
            info);
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
        return ParseJournal(journalPath, journalBytes);
    }

    private static async Task<JournalReadResult> ReadJournalAsync(string journalPath, FileStream stream)
    {
        if (stream.Length > int.MaxValue)
        {
            throw new InvalidOperationException($"Transaction journal is too large to validate: {journalPath}");
        }
        byte[] journalBytes = new byte[(int)stream.Length];
        stream.Seek(0, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(journalBytes);
        return ParseJournal(journalPath, journalBytes);
    }

    private static JournalReadResult ParseJournal(string journalPath, byte[] journalBytes)
    {
        bool missingTerminalLf = journalBytes.Length > 0 && journalBytes[^1] != (byte)'\n';
        string journalText = Encoding.UTF8.GetString(journalBytes);
        string? operationId = null;
        int lastSequence = 0;
        string state = "recoveryRequired";
        string lastValidState = "none";
        bool invalidTail = false;
        bool sawRecord = false;
        bool sawTerminal = false;
        string? declaredBackupDir = null;
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
                    string? preparedBackupDir = root.TryGetProperty("backupDir", out JsonElement backupDirValue)
                        ? backupDirValue.GetString()
                        : null;
                    if (string.IsNullOrWhiteSpace(preparedBackupDir)
                        || !Path.IsPathFullyQualified(preparedBackupDir))
                    {
                        throw new InvalidOperationException(
                            "Prepared journal record must declare an absolute backupDir.");
                    }
                    declaredBackupDir = Path.GetFullPath(preparedBackupDir);
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
            declaredBackupDir,
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
    string? DeclaredBackupDir,
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

    internal RecoveryRequiredException(string message, IReadOnlyList<string> pendingBackupDirectories)
        : base(message)
    {
        PendingBackupDirectories = [.. pendingBackupDirectories];
    }

    public string Code => "RECOVERY_REQUIRED";

    public IReadOnlyList<string> PendingBackupDirectories { get; }
}
