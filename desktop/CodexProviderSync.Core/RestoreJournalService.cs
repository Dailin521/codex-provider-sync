using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CodexProviderSync.Core;

internal sealed record RestoreBackupIdentity(
    [property: JsonPropertyName("backupId")] string BackupId,
    [property: JsonPropertyName("backupDir")] string BackupDir,
    [property: JsonPropertyName("revision")] string Revision);

internal sealed record RestorePreSnapshotIdentity(
    [property: JsonPropertyName("backupId")] string BackupId,
    [property: JsonPropertyName("backupDir")] string BackupDir,
    [property: JsonPropertyName("revision")] string Revision,
    [property: JsonPropertyName("manifestSha256")] string ManifestSha256);

internal sealed record RestoreStorageIdentity(
    [property: JsonPropertyName("codexHome")] string CodexHome,
    [property: JsonPropertyName("codexHomePhysical")] string CodexHomePhysical,
    [property: JsonPropertyName("sqliteHome")] string? SqliteHome,
    [property: JsonPropertyName("stateDbResourceKey")] string? StateDbResourceKey,
    [property: JsonPropertyName("targetStateDbPath")] string? TargetStateDbPath);

internal sealed record RestoreDigest(
    [property: JsonPropertyName("present")] bool Present,
    [property: JsonPropertyName("digestKind")] string DigestKind,
    [property: JsonPropertyName("digest")] string Digest,
    [property: JsonPropertyName("sizeBytes")] long? SizeBytes = null);

internal sealed record RestoreJournalTarget(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("kind")] string Kind,
    [property: JsonPropertyName("targetPath")] string TargetPath,
    [property: JsonPropertyName("pre")] RestoreDigest Pre,
    [property: JsonPropertyName("expectedPost")] RestoreDigest ExpectedPost,
    [property: JsonPropertyName("snapshotPath")] string? SnapshotPath = null,
    [property: JsonPropertyName("snapshotEntryIndex")] int? SnapshotEntryIndex = null);

internal sealed record RestoreJournalPrepared(
    RestoreBackupIdentity SourceBackup,
    RestorePreSnapshotIdentity PreRestoreSnapshot,
    RestoreStorageIdentity Storage,
    IReadOnlyList<string> RequiredTargetKinds,
    IReadOnlyList<string> ResolvesOperationIds,
    IReadOnlyList<RestoreJournalTarget> Targets);

internal sealed record RestoreJournalEvent(
    int SchemaVersion,
    int ProtocolVersion,
    string OperationKind,
    string OperationId,
    int Sequence,
    string State,
    DateTimeOffset RecordedAt,
    string? TargetId = null,
    string? TargetPhase = null,
    string? TargetDigest = null,
    string? PostManifestSha256 = null,
    string? ReasonCode = null);

internal sealed record RestoreJournalProtectionReferences(
    string SnapshotDirectory,
    string? SourceBackupDirectory,
    string? PreRestoreSnapshotDirectory,
    bool IsUnverifiable);

internal sealed record RestoreJournalInfo(
    string JournalPath,
    string SnapshotDir,
    string BackupDir,
    string? OperationId,
    string State,
    RestoreJournalPrepared? Prepared,
    IReadOnlyList<RestoreJournalEvent> Events,
    int LastSequence,
    bool InvalidTail,
    string? ValidationError,
    bool Terminal,
    bool Blocking,
    IReadOnlyDictionary<string, string> TargetPhases,
    RestoreJournalProtectionReferences ProtectionReferences);

internal sealed record RestoreJournalScan(
    IReadOnlyList<RestoreJournalInfo> Journals,
    IReadOnlyList<RestoreJournalInfo> BlockingJournals,
    IReadOnlySet<string> ResolvedOperationIds,
    IReadOnlySet<string> ProtectedDirectories,
    bool PruneReferencesUnverifiable);

internal sealed class RestoreJournal
{
    internal const string FileName = "restore-journal.v2.jsonl";
    internal const int SchemaVersion = 2;
    internal const int ProtocolVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly SemaphoreSlim _appendGate = new(1, 1);
    private readonly string _filePath;
    private readonly string _operationId;
    private int _sequence;

    private RestoreJournal(string filePath, string operationId, int sequence)
    {
        _filePath = Path.GetFullPath(filePath);
        _operationId = operationId;
        _sequence = sequence;
    }

    internal string FilePath => _filePath;

    internal static async Task<RestoreJournal> CreateAsync(
        string snapshotDir,
        string operationId,
        RestoreJournalPrepared prepared,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(prepared);
        ValidatePrepared(prepared);

        string directory = Path.GetFullPath(snapshotDir);
        Directory.CreateDirectory(directory);
        string filePath = Path.Combine(directory, FileName);
        Dictionary<string, object?> value = BaseEvent(operationId, 1, "prepared");
        value["sourceBackup"] = prepared.SourceBackup;
        value["preRestoreSnapshot"] = prepared.PreRestoreSnapshot;
        value["storage"] = prepared.Storage;
        value["requiredTargetKinds"] = prepared.RequiredTargetKinds;
        value["resolvesOperationIds"] = prepared.ResolvesOperationIds;
        value["targets"] = prepared.Targets;
        byte[] bytes = SerializeLine(value);

        await using (FileStream stream = new(
            filePath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.Read,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.WriteThrough))
        {
            await stream.WriteAsync(bytes, cancellationToken);
            await stream.FlushAsync(cancellationToken);
            stream.Flush(flushToDisk: true);
        }
        RestoreJournalDurability.SyncDirectory(directory);

        RestoreJournalInfo verified = await RestoreJournalService.ReadInfoAsync(filePath, cancellationToken);
        if (verified.InvalidTail
            || verified.State != "prepared"
            || verified.OperationId != operationId
            || verified.LastSequence != 1)
        {
            throw new InvalidOperationException("Restore journal prepared event did not persist durably.");
        }
        return new RestoreJournal(filePath, operationId, 1);
    }

    internal static RestoreJournal Reopen(RestoreJournalInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        if (info.InvalidTail || string.IsNullOrWhiteSpace(info.OperationId))
        {
            throw new InvalidOperationException("Cannot reopen an invalid Restore journal.");
        }
        return new RestoreJournal(info.JournalPath, info.OperationId, info.LastSequence);
    }

    internal Task ApplyingAsync(CancellationToken cancellationToken = default) =>
        AppendAsync("applying", null, cancellationToken);

    internal Task TargetIntentAsync(string targetId, CancellationToken cancellationToken = default) =>
        AppendAsync("applying", new Dictionary<string, object?>
        {
            ["targetId"] = targetId,
            ["targetPhase"] = "intent"
        }, cancellationToken);

    internal Task TargetCompletedAsync(
        string targetId,
        string targetDigest,
        CancellationToken cancellationToken = default) =>
        AppendAsync("applying", new Dictionary<string, object?>
        {
            ["targetId"] = targetId,
            ["targetPhase"] = "completed",
            ["targetDigest"] = targetDigest
        }, cancellationToken);

    internal Task CommittingAsync(string postManifestSha256, CancellationToken cancellationToken = default) =>
        AppendAsync("committing", new Dictionary<string, object?>
        {
            ["postManifestSha256"] = postManifestSha256
        }, cancellationToken);

    internal Task CommittedPendingAckAsync(
        string postManifestSha256,
        CancellationToken cancellationToken = default) =>
        AppendAsync("committed-pending-ack", new Dictionary<string, object?>
        {
            ["postManifestSha256"] = postManifestSha256
        }, cancellationToken);

    internal Task CompletedAsync(CancellationToken cancellationToken = default) =>
        AppendAsync("completed", null, cancellationToken);

    internal Task RollbackPendingAsync(string reasonCode, CancellationToken cancellationToken = default) =>
        AppendAsync("rollback-pending", new Dictionary<string, object?>
        {
            ["reasonCode"] = reasonCode
        }, cancellationToken);

    internal Task TargetCompensatedAsync(
        string targetId,
        string targetDigest,
        CancellationToken cancellationToken = default) =>
        AppendAsync("rollback-pending", new Dictionary<string, object?>
        {
            ["targetId"] = targetId,
            ["targetPhase"] = "compensated",
            ["targetDigest"] = targetDigest
        }, cancellationToken);

    internal Task RolledBackAsync(CancellationToken cancellationToken = default) =>
        AppendAsync("rolled-back", null, cancellationToken);

    internal Task RecoveryRequiredAsync(string reasonCode, CancellationToken cancellationToken = default) =>
        AppendAsync("recovery-required", new Dictionary<string, object?>
        {
            ["reasonCode"] = reasonCode
        }, cancellationToken);

    private async Task AppendAsync(
        string state,
        IReadOnlyDictionary<string, object?>? details,
        CancellationToken cancellationToken)
    {
        await _appendGate.WaitAsync(cancellationToken);
        try
        {
            RestoreJournalInfo before = await RestoreJournalService.ReadInfoAsync(_filePath, cancellationToken);
            if (before.InvalidTail
                || before.OperationId != _operationId
                || before.LastSequence != _sequence)
            {
                throw new InvalidOperationException("Restore journal changed before append.");
            }
            RestoreJournalService.ValidateAppend(before, state, details);

            int nextSequence = checked(_sequence + 1);
            Dictionary<string, object?> value = BaseEvent(_operationId, nextSequence, state);
            if (details is not null)
            {
                foreach ((string name, object? detail) in details)
                {
                    value[name] = detail;
                }
            }
            byte[] bytes = SerializeLine(value);
            try
            {
                await using FileStream stream = new(
                    _filePath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.Asynchronous | FileOptions.WriteThrough);
                await stream.WriteAsync(bytes, cancellationToken);
                await stream.FlushAsync(cancellationToken);
                stream.Flush(flushToDisk: true);
            }
            catch
            {
                try
                {
                    RestoreJournalInfo reconciled = await RestoreJournalService.ReadInfoAsync(
                        _filePath,
                        CancellationToken.None);
                    if (!reconciled.InvalidTail
                        && reconciled.OperationId == _operationId
                        && reconciled.LastSequence == nextSequence
                        && reconciled.State == state)
                    {
                        _sequence = nextSequence;
                    }
                }
                catch
                {
                    // Preserve the original append failure.
                }
                throw;
            }

            RestoreJournalInfo after = await RestoreJournalService.ReadInfoAsync(_filePath, cancellationToken);
            if (after.InvalidTail
                || after.OperationId != _operationId
                || after.LastSequence != nextSequence
                || after.State != state)
            {
                throw new InvalidOperationException("Restore journal append could not be verified.");
            }
            _sequence = nextSequence;
        }
        finally
        {
            _appendGate.Release();
        }
    }

    private static Dictionary<string, object?> BaseEvent(
        string operationId,
        int sequence,
        string state) => new()
    {
        ["schemaVersion"] = SchemaVersion,
        ["protocolVersion"] = ProtocolVersion,
        ["operationKind"] = "restore",
        ["operationId"] = operationId,
        ["sequence"] = sequence,
        ["state"] = state,
        ["recordedAt"] = DateTimeOffset.UtcNow.ToString("O")
    };

    private static byte[] SerializeLine(Dictionary<string, object?> value) =>
        new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true)
            .GetBytes(JsonSerializer.Serialize(value, JsonOptions) + "\n");

    internal static void ValidatePrepared(RestoreJournalPrepared prepared)
    {
        RestoreJournalService.ValidateIdentity(prepared.SourceBackup.BackupId, prepared.SourceBackup.BackupDir, prepared.SourceBackup.Revision);
        RestoreJournalService.ValidateIdentity(
            prepared.PreRestoreSnapshot.BackupId,
            prepared.PreRestoreSnapshot.BackupDir,
            prepared.PreRestoreSnapshot.Revision);
        if (string.IsNullOrWhiteSpace(prepared.PreRestoreSnapshot.ManifestSha256))
        {
            throw new InvalidOperationException("Restore snapshot manifest digest is required.");
        }
        if (!Path.IsPathFullyQualified(prepared.Storage.CodexHome))
        {
            throw new InvalidOperationException("Restore storage Codex Home must be absolute.");
        }
        if (!Path.IsPathFullyQualified(prepared.Storage.CodexHomePhysical))
        {
            throw new InvalidOperationException("Restore storage physical Codex Home must be absolute.");
        }
        if (prepared.Storage.SqliteHome is not null
            && !Path.IsPathFullyQualified(prepared.Storage.SqliteHome))
        {
            throw new InvalidOperationException("Restore storage SQLite Home must be absolute.");
        }
        if (prepared.Storage.TargetStateDbPath is not null
            && !Path.IsPathFullyQualified(prepared.Storage.TargetStateDbPath))
        {
            throw new InvalidOperationException("Restore State DB target must be absolute.");
        }
        if (prepared.Targets.Select(static target => target.Id).Distinct(StringComparer.Ordinal).Count()
            != prepared.Targets.Count)
        {
            throw new InvalidOperationException("Restore target identifiers must be unique.");
        }
        if (prepared.Targets.Count == 0)
        {
            throw new InvalidOperationException("Restore journal must declare at least one target.");
        }
        foreach (RestoreJournalTarget target in prepared.Targets)
        {
            if (string.IsNullOrWhiteSpace(target.Id)
                || string.IsNullOrWhiteSpace(target.Kind)
                || !Path.IsPathFullyQualified(target.TargetPath))
            {
                throw new InvalidOperationException("Restore target declaration is invalid.");
            }
            RestoreJournalService.ValidateDigest(target.Pre);
            RestoreJournalService.ValidateDigest(target.ExpectedPost);
        }
        if (prepared.RequiredTargetKinds.Any(string.IsNullOrWhiteSpace)
            || prepared.ResolvesOperationIds.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException("Restore journal contains an empty required identifier.");
        }
        HashSet<string> targetKinds = prepared.Targets
            .Select(static target => target.Kind)
            .ToHashSet(StringComparer.Ordinal);
        HashSet<string> requiredKinds = prepared.RequiredTargetKinds.ToHashSet(StringComparer.Ordinal);
        if (requiredKinds.Count != prepared.RequiredTargetKinds.Count
            || !targetKinds.SetEquals(requiredKinds))
        {
            throw new InvalidOperationException(
                "Restore required target kinds must exactly match the declared targets.");
        }
    }
}

internal static class RestoreJournalService
{
    private static readonly HashSet<string> ValidStates = new(StringComparer.Ordinal)
    {
        "prepared",
        "applying",
        "committing",
        "committed-pending-ack",
        "completed",
        "rollback-pending",
        "rolled-back",
        "recovery-required"
    };

    private static readonly IReadOnlyDictionary<string, HashSet<string>> ValidTransitions =
        new Dictionary<string, HashSet<string>>(StringComparer.Ordinal)
        {
            ["prepared"] = new(StringComparer.Ordinal) { "applying", "rollback-pending", "recovery-required" },
            ["applying"] = new(StringComparer.Ordinal) { "applying", "committing", "rollback-pending", "recovery-required" },
            ["committing"] = new(StringComparer.Ordinal) { "committed-pending-ack", "rollback-pending", "recovery-required" },
            ["committed-pending-ack"] = new(StringComparer.Ordinal) { "completed", "recovery-required" },
            ["rollback-pending"] = new(StringComparer.Ordinal) { "rollback-pending", "rolled-back", "recovery-required" },
            ["completed"] = new(StringComparer.Ordinal),
            ["rolled-back"] = new(StringComparer.Ordinal),
            ["recovery-required"] = new(StringComparer.Ordinal)
        };

    internal static async Task<RestoreJournalInfo> ReadInfoAsync(
        string journalPath,
        CancellationToken cancellationToken = default)
    {
        byte[] bytes = await File.ReadAllBytesAsync(journalPath, cancellationToken);
        return Parse(Path.GetFullPath(journalPath), bytes);
    }

    internal static async Task<IReadOnlyList<RestoreJournalInfo>> FindAsync(
        string codexHome,
        CancellationToken cancellationToken = default)
    {
        string root = new CodexHomeService().BackupRoot(codexHome);
        if (!Directory.Exists(root))
        {
            return [];
        }
        List<RestoreJournalInfo> journals = [];
        foreach (string directory in Directory.EnumerateDirectories(root).Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string journalPath = Path.Combine(directory, RestoreJournal.FileName);
            if (!File.Exists(journalPath))
            {
                continue;
            }
            try
            {
                journals.Add(await ReadInfoAsync(journalPath, cancellationToken));
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                string fullDirectory = Path.GetFullPath(directory);
                journals.Add(new RestoreJournalInfo(
                    Path.GetFullPath(journalPath),
                    fullDirectory,
                    fullDirectory,
                    null,
                    "recovery-required",
                    null,
                    [],
                    0,
                    true,
                    error.Message,
                    false,
                    true,
                    new Dictionary<string, string>(StringComparer.Ordinal),
                    new RestoreJournalProtectionReferences(
                        fullDirectory,
                        null,
                        fullDirectory,
                        true)));
            }
        }
        return journals.OrderBy(static item => item.JournalPath, StringComparer.Ordinal).ToArray();
    }

    internal static async Task<RestoreJournalScan> ScanAsync(
        string codexHome,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<RestoreJournalInfo> journals = await FindAsync(codexHome, cancellationToken);
        Dictionary<string, RestoreJournalInfo[]> journalsByOperationId = journals
            .Where(static journal => !string.IsNullOrWhiteSpace(journal.OperationId))
            .GroupBy(static journal => journal.OperationId!, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => group.ToArray(),
                StringComparer.Ordinal);
        HashSet<string> resolvedOperationIds = new(StringComparer.Ordinal);
        foreach (RestoreJournalInfo resolver in journals.Where(static journal =>
                     !journal.InvalidTail
                     && journal.State == "completed"
                     && journal.Prepared is not null))
        {
            foreach (string operationId in resolver.Prepared!.ResolvesOperationIds)
            {
                if (!journalsByOperationId.TryGetValue(operationId, out RestoreJournalInfo[]? matches)
                    || matches.Length != 1
                    || !ResolverCanResolve(resolver, matches[0]))
                {
                    continue;
                }
                resolvedOperationIds.Add(operationId);
            }
        }
        RestoreJournalInfo[] blocking = journals
            .Where(journal => journal.Blocking
                && (string.IsNullOrWhiteSpace(journal.OperationId)
                    || !resolvedOperationIds.Contains(journal.OperationId)))
            .ToArray();
        HashSet<string> protectedDirectories = new(PathComparer);
        bool unverifiable = false;
        // Resolution may admit a later explicit Restore, but it is not
        // authority to delete evidence. Protect every nonterminal journal's
        // source and pre-snapshot until that journal itself is terminal.
        foreach (RestoreJournalInfo journal in journals.Where(static item => item.Blocking))
        {
            RestoreJournalProtectionReferences references = journal.ProtectionReferences;
            protectedDirectories.Add(Path.GetFullPath(references.SnapshotDirectory));
            if (!string.IsNullOrWhiteSpace(references.SourceBackupDirectory))
            {
                protectedDirectories.Add(Path.GetFullPath(references.SourceBackupDirectory));
            }
            if (!string.IsNullOrWhiteSpace(references.PreRestoreSnapshotDirectory))
            {
                protectedDirectories.Add(Path.GetFullPath(references.PreRestoreSnapshotDirectory));
            }
            unverifiable |= references.IsUnverifiable;
        }
        return new RestoreJournalScan(
            journals,
            blocking,
            resolvedOperationIds,
            protectedDirectories,
            unverifiable);
    }

    private static bool ResolverCanResolve(
        RestoreJournalInfo resolver,
        RestoreJournalInfo pending)
    {
        RestoreJournalPrepared? resolverPrepared = resolver.Prepared;
        RestoreJournalPrepared? pendingPrepared = pending.Prepared;
        if (!pending.Blocking
            || pending.InvalidTail
            || resolverPrepared is null
            || pendingPrepared is null)
        {
            return false;
        }
        RestoreBackupIdentity resolverSource = resolverPrepared.SourceBackup;
        RestoreBackupIdentity pendingSource = pendingPrepared.SourceBackup;
        string? resolverSourcePath = TryPhysicalDirectoryPathKey(resolverSource.BackupDir);
        string? pendingSourcePath = TryPhysicalDirectoryPathKey(pendingSource.BackupDir);
        string? resolverHomePath = TryPhysicalDirectoryPathKey(resolverPrepared.Storage.CodexHome);
        string? pendingHomePath = TryPhysicalDirectoryPathKey(pendingPrepared.Storage.CodexHome);
        string? resolverRecordedHomePath = TryPersistedPhysicalPathKey(
            resolverPrepared.Storage.CodexHomePhysical);
        string? pendingRecordedHomePath = TryPersistedPhysicalPathKey(
            pendingPrepared.Storage.CodexHomePhysical);
        if (resolverSourcePath is null
            || pendingSourcePath is null
            || !PathComparer.Equals(resolverSourcePath, pendingSourcePath)
            || !string.Equals(resolverSource.Revision, pendingSource.Revision, StringComparison.Ordinal)
            || resolverHomePath is null
            || pendingHomePath is null
            || resolverRecordedHomePath is null
            || pendingRecordedHomePath is null
            || !PathComparer.Equals(resolverHomePath, pendingHomePath)
            || !PathComparer.Equals(resolverHomePath, resolverRecordedHomePath)
            || !PathComparer.Equals(pendingHomePath, pendingRecordedHomePath))
        {
            return false;
        }
        HashSet<string> resolverKinds = resolverPrepared.RequiredTargetKinds
            .ToHashSet(StringComparer.Ordinal);
        return pendingPrepared.RequiredTargetKinds.All(resolverKinds.Contains);
    }

    private static string? TryPhysicalDirectoryPathKey(string path)
    {
        try
        {
            string first = StateDbLockResource.ResolveExistingPhysicalPath(
                Path.GetFullPath(path),
                directory: true);
            string second = StateDbLockResource.ResolveExistingPhysicalPath(
                Path.GetFullPath(path),
                directory: true);
            return PathComparer.Equals(first, second) ? Path.GetFullPath(first) : null;
        }
        catch (Exception error) when (error is IOException
            or UnauthorizedAccessException
            or System.ComponentModel.Win32Exception
            or ArgumentException
            or NotSupportedException
            or System.Security.SecurityException)
        {
            return null;
        }
    }

    private static string? TryPersistedPhysicalPathKey(string path)
    {
        try
        {
            return Path.IsPathFullyQualified(path) ? Path.GetFullPath(path) : null;
        }
        catch (Exception error) when (error is ArgumentException
            or NotSupportedException
            or System.Security.SecurityException)
        {
            return null;
        }
    }

    internal static async Task<IReadOnlyList<RestoreJournalInfo>> FindBlockingAsync(
        string codexHome,
        CancellationToken cancellationToken = default) =>
        (await ScanAsync(codexHome, cancellationToken)).BlockingJournals;

    internal static void ValidateAppend(
        RestoreJournalInfo before,
        string nextState,
        IReadOnlyDictionary<string, object?>? details)
    {
        if (!ValidTransitions.TryGetValue(before.State, out HashSet<string>? allowed)
            || !allowed.Contains(nextState))
        {
            throw new InvalidOperationException(
                $"Restore journal transition {before.State} -> {nextState} is invalid.");
        }
        string? targetId = ReadDetail(details, "targetId");
        string? targetPhase = ReadDetail(details, "targetPhase");
        string? targetDigest = ReadDetail(details, "targetDigest");
        ValidateTargetTransition(before.Prepared, before.TargetPhases, nextState, targetId, targetPhase, targetDigest);
        Dictionary<string, string> phases = new(before.TargetPhases, StringComparer.Ordinal);
        if (targetId is not null && targetPhase is not null)
        {
            phases[targetId] = targetPhase;
        }
        string? postManifestSha256 = ReadDetail(details, "postManifestSha256");
        string? committingHash = before.Events
            .LastOrDefault(static item => item.State == "committing")
            ?.PostManifestSha256;
        ValidateStateEvidence(
            before.Prepared,
            phases,
            nextState,
            postManifestSha256,
            committingHash);
        if (nextState is "committing" or "committed-pending-ack"
            && string.IsNullOrWhiteSpace(postManifestSha256))
        {
            throw new InvalidOperationException("Restore commit event requires postManifestSha256.");
        }
    }

    private static RestoreJournalInfo Parse(string journalPath, byte[] bytes)
    {
        string snapshotDir = Path.GetDirectoryName(journalPath)
            ?? throw new InvalidOperationException("Restore journal has no parent directory.");
        bool missingLf = bytes.Length > 0 && bytes[^1] != (byte)'\n';
        string text;
        try
        {
            text = new UTF8Encoding(false, true).GetString(bytes);
        }
        catch (DecoderFallbackException error)
        {
            return Invalid(journalPath, snapshotDir, null, null, [], 0, new Dictionary<string, string>(),
                $"Restore journal is not valid UTF-8: {error.Message}", null, null, true);
        }

        List<RestoreJournalEvent> events = [];
        string? operationId = null;
        string? state = null;
        RestoreJournalPrepared? prepared = null;
        Dictionary<string, string> phases = new(StringComparer.Ordinal);
        string? committingHash = null;
        int expectedSequence = 1;
        string? validationError = missingLf
            ? "Restore journal is missing its final newline and may contain a torn append."
            : null;
        string? rawSourceBackupDir = null;
        string? rawPreSnapshotDir = null;

        foreach (string line in text.Split('\n'))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            JsonDocument document;
            try
            {
                document = JsonDocument.Parse(line);
            }
            catch (JsonException)
            {
                validationError ??= "Restore journal contains a truncated or malformed JSON line.";
                break;
            }
            using (document)
            {
                JsonElement root = document.RootElement;
                if (events.Count == 0 && root.ValueKind == JsonValueKind.Object)
                {
                    rawSourceBackupDir = TryAbsoluteNestedPath(root, "sourceBackup", "backupDir");
                    rawPreSnapshotDir = TryAbsoluteNestedPath(root, "preRestoreSnapshot", "backupDir");
                }
                try
                {
                    if (root.ValueKind != JsonValueKind.Object)
                    {
                        throw new InvalidOperationException("Restore journal event is not an object.");
                    }
                    int schemaVersion = RequiredInt(root, "schemaVersion");
                    int protocolVersion = RequiredInt(root, "protocolVersion");
                    string operationKind = RequiredString(root, "operationKind");
                    string currentOperationId = RequiredString(root, "operationId");
                    int sequence = RequiredInt(root, "sequence");
                    string currentState = RequiredString(root, "state");
                    string recordedAtText = RequiredString(root, "recordedAt");
                    if (schemaVersion != RestoreJournal.SchemaVersion
                        || protocolVersion != RestoreJournal.ProtocolVersion
                        || operationKind != "restore"
                        || !ValidStates.Contains(currentState))
                    {
                        throw new InvalidOperationException(
                            "Restore journal event has an unsupported schema, protocol, kind, or state.");
                    }
                    if (sequence != expectedSequence)
                    {
                        throw new InvalidOperationException(
                            $"Restore journal sequence mismatch: expected {expectedSequence}, received {sequence}.");
                    }
                    if (!DateTimeOffset.TryParse(recordedAtText, out DateTimeOffset recordedAt))
                    {
                        throw new InvalidOperationException("Restore journal recordedAt is invalid.");
                    }
                    if (operationId is null)
                    {
                        if (currentState != "prepared")
                        {
                            throw new InvalidOperationException("Restore journal must start with prepared.");
                        }
                        prepared = ParsePrepared(root);
                        RestoreJournal.ValidatePrepared(prepared);
                        operationId = currentOperationId;
                        state = currentState;
                    }
                    else
                    {
                        if (currentOperationId != operationId)
                        {
                            throw new InvalidOperationException("Restore journal operationId changed.");
                        }
                        if (state is null
                            || !ValidTransitions.TryGetValue(state, out HashSet<string>? allowed)
                            || !allowed.Contains(currentState))
                        {
                            throw new InvalidOperationException(
                                $"Restore journal transition {state} -> {currentState} is invalid.");
                        }
                        state = currentState;
                    }

                    string? targetId = OptionalString(root, "targetId");
                    string? targetPhase = OptionalString(root, "targetPhase");
                    string? targetDigest = OptionalString(root, "targetDigest");
                    string? postManifestSha256 = OptionalString(root, "postManifestSha256");
                    string? reasonCode = OptionalString(root, "reasonCode");
                    ValidateTargetTransition(prepared, phases, currentState, targetId, targetPhase, targetDigest);
                    if (currentState is "committing" or "committed-pending-ack"
                        && string.IsNullOrWhiteSpace(postManifestSha256))
                    {
                        throw new InvalidOperationException("Restore commit event requires postManifestSha256.");
                    }
                    if (targetId is not null && targetPhase is not null)
                    {
                        phases[targetId] = targetPhase;
                    }
                    ValidateStateEvidence(
                        prepared,
                        phases,
                        currentState,
                        postManifestSha256,
                        committingHash);
                    if (currentState == "committing")
                    {
                        committingHash = postManifestSha256;
                    }
                    events.Add(new RestoreJournalEvent(
                        schemaVersion,
                        protocolVersion,
                        operationKind,
                        currentOperationId,
                        sequence,
                        currentState,
                        recordedAt,
                        targetId,
                        targetPhase,
                        targetDigest,
                        postManifestSha256,
                        reasonCode));
                    expectedSequence++;
                }
                catch (Exception error)
                {
                    validationError ??= error.Message;
                    break;
                }
            }
        }

        if (events.Count == 0)
        {
            validationError ??= "Restore journal contains no valid events.";
        }
        if (validationError is not null)
        {
            return Invalid(
                journalPath,
                snapshotDir,
                operationId,
                prepared,
                events,
                events.Count == 0 ? 0 : events[^1].Sequence,
                phases,
                validationError,
                rawSourceBackupDir,
                rawPreSnapshotDir,
                rawSourceBackupDir is null || rawPreSnapshotDir is null);
        }

        string finalState = state ?? "recovery-required";
        bool terminal = finalState is "completed" or "rolled-back" or "recovery-required";
        bool blocking = finalState is not ("completed" or "rolled-back");
        return new RestoreJournalInfo(
            journalPath,
            snapshotDir,
            snapshotDir,
            operationId,
            finalState,
            prepared,
            events,
            events[^1].Sequence,
            false,
            null,
            terminal,
            blocking,
            phases,
            new RestoreJournalProtectionReferences(
                snapshotDir,
                prepared?.SourceBackup.BackupDir,
                prepared?.PreRestoreSnapshot.BackupDir ?? snapshotDir,
                false));
    }

    private static RestoreJournalInfo Invalid(
        string journalPath,
        string snapshotDir,
        string? operationId,
        RestoreJournalPrepared? prepared,
        IReadOnlyList<RestoreJournalEvent> events,
        int lastSequence,
        IReadOnlyDictionary<string, string> phases,
        string validationError,
        string? rawSourceBackupDir,
        string? rawPreSnapshotDir,
        bool referencesUnverifiable) => new(
            journalPath,
            snapshotDir,
            snapshotDir,
            operationId,
            "recovery-required",
            prepared,
            events,
            lastSequence,
            true,
            validationError,
            false,
            true,
            phases,
            new RestoreJournalProtectionReferences(
                snapshotDir,
                rawSourceBackupDir ?? prepared?.SourceBackup.BackupDir,
                rawPreSnapshotDir ?? prepared?.PreRestoreSnapshot.BackupDir ?? snapshotDir,
                referencesUnverifiable));

    private static RestoreJournalPrepared ParsePrepared(JsonElement root)
    {
        RestoreBackupIdentity source = ParseIdentity(root.GetProperty("sourceBackup"));
        JsonElement preElement = root.GetProperty("preRestoreSnapshot");
        RestoreBackupIdentity preBase = ParseIdentity(preElement);
        RestorePreSnapshotIdentity pre = new(
            preBase.BackupId,
            preBase.BackupDir,
            preBase.Revision,
            RequiredString(preElement, "manifestSha256"));
        JsonElement storageElement = root.GetProperty("storage");
        RestoreStorageIdentity storage = new(
            RequiredString(storageElement, "codexHome"),
            RequiredString(storageElement, "codexHomePhysical"),
            OptionalString(storageElement, "sqliteHome"),
            OptionalString(storageElement, "stateDbResourceKey"),
            OptionalString(storageElement, "targetStateDbPath"));
        string[] requiredTargetKinds = RequiredStringArray(root, "requiredTargetKinds");
        string[] resolvesOperationIds = root.TryGetProperty("resolvesOperationIds", out JsonElement resolves)
            ? ParseStringArray(resolves, "resolvesOperationIds")
            : [];
        JsonElement targetsElement = root.GetProperty("targets");
        if (targetsElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Restore targets must be an array.");
        }
        List<RestoreJournalTarget> targets = [];
        foreach (JsonElement target in targetsElement.EnumerateArray())
        {
            targets.Add(new RestoreJournalTarget(
                RequiredString(target, "id"),
                RequiredString(target, "kind"),
                RequiredString(target, "targetPath"),
                ParseDigest(target.GetProperty("pre")),
                ParseDigest(target.GetProperty("expectedPost")),
                OptionalString(target, "snapshotPath"),
                OptionalInt(target, "snapshotEntryIndex")));
        }
        return new RestoreJournalPrepared(source, pre, storage, requiredTargetKinds, resolvesOperationIds, targets);
    }

    private static RestoreBackupIdentity ParseIdentity(JsonElement value) => new(
        RequiredString(value, "backupId"),
        RequiredString(value, "backupDir"),
        RequiredString(value, "revision"));

    private static RestoreDigest ParseDigest(JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Object
            || !value.TryGetProperty("present", out JsonElement present)
            || present.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
        {
            throw new InvalidOperationException("Restore digest present flag is invalid.");
        }
        RestoreDigest digest = new(
            present.GetBoolean(),
            RequiredString(value, "digestKind"),
            RequiredString(value, "digest"),
            OptionalLong(value, "sizeBytes"));
        ValidateDigest(digest);
        return digest;
    }

    internal static void ValidateDigest(RestoreDigest digest)
    {
        if (string.IsNullOrWhiteSpace(digest.DigestKind)
            || string.IsNullOrWhiteSpace(digest.Digest)
            || digest.SizeBytes < 0)
        {
            throw new InvalidOperationException("Restore digest is invalid.");
        }
    }

    internal static void ValidateIdentity(string backupId, string backupDir, string revision)
    {
        if (string.IsNullOrWhiteSpace(backupId)
            || !Path.IsPathFullyQualified(backupDir)
            || string.IsNullOrWhiteSpace(revision))
        {
            throw new InvalidOperationException("Restore backup identity is invalid.");
        }
    }

    private static void ValidateTargetTransition(
        RestoreJournalPrepared? prepared,
        IReadOnlyDictionary<string, string> phases,
        string state,
        string? targetId,
        string? targetPhase,
        string? targetDigest)
    {
        if (targetId is null && targetPhase is null && targetDigest is null)
        {
            return;
        }
        if (targetId is null
            || targetPhase is null
            || prepared is null
            || !prepared.Targets.Any(target => target.Id == targetId))
        {
            throw new InvalidOperationException("Restore journal target transition is malformed or undeclared.");
        }
        phases.TryGetValue(targetId, out string? previous);
        RestoreJournalTarget target = prepared.Targets.Single(item => item.Id == targetId);
        switch (targetPhase)
        {
            case "intent" when state == "applying" && previous is null:
                return;
            case "completed" when state == "applying"
                && previous == "intent"
                && targetDigest == target.ExpectedPost.Digest:
                return;
            case "compensated" when state == "rollback-pending"
                && previous != "compensated"
                && targetDigest == target.Pre.Digest:
                return;
            default:
                throw new InvalidOperationException("Restore journal target transition is invalid.");
        }
    }

    private static void ValidateStateEvidence(
        RestoreJournalPrepared? prepared,
        IReadOnlyDictionary<string, string> phases,
        string state,
        string? postManifestSha256,
        string? committingHash)
    {
        if (prepared is null)
        {
            return;
        }
        if (state == "committing")
        {
            if (string.IsNullOrWhiteSpace(postManifestSha256)
                || prepared.Targets.Any(target =>
                    !phases.TryGetValue(target.Id, out string? phase) || phase != "completed"))
            {
                throw new InvalidOperationException(
                    "Restore cannot commit before every declared target is completed.");
            }
        }
        else if (state == "committed-pending-ack")
        {
            if (string.IsNullOrWhiteSpace(postManifestSha256)
                || postManifestSha256 != committingHash)
            {
                throw new InvalidOperationException(
                    "Restore commit acknowledgement hash does not match committing evidence.");
            }
        }
        else if (state == "rolled-back"
            && prepared.Targets.Any(target =>
                !phases.TryGetValue(target.Id, out string? phase) || phase != "compensated"))
        {
            throw new InvalidOperationException(
                "Restore cannot become rolled-back before every declared target is compensated.");
        }
    }

    private static string? ReadDetail(IReadOnlyDictionary<string, object?>? details, string name) =>
        details is not null
        && details.TryGetValue(name, out object? value)
        && value is string text
            ? text
            : null;

    private static string RequiredString(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property)
            || property.ValueKind != JsonValueKind.String
            || string.IsNullOrWhiteSpace(property.GetString()))
        {
            throw new InvalidOperationException($"Restore journal field {name} is required.");
        }
        return property.GetString()!;
    }

    private static string? OptionalString(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property)
            || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }
        if (property.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException($"Restore journal field {name} must be a string.");
        }
        return property.GetString();
    }

    private static int RequiredInt(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property)
            || property.ValueKind != JsonValueKind.Number
            || !property.TryGetInt32(out int result))
        {
            throw new InvalidOperationException($"Restore journal field {name} must be an integer.");
        }
        return result;
    }

    private static int? OptionalInt(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property)
            || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }
        if (property.ValueKind != JsonValueKind.Number || !property.TryGetInt32(out int result))
        {
            throw new InvalidOperationException($"Restore journal field {name} must be an integer.");
        }
        return result;
    }

    private static long? OptionalLong(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property)
            || property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }
        if (property.ValueKind != JsonValueKind.Number || !property.TryGetInt64(out long result))
        {
            throw new InvalidOperationException($"Restore journal field {name} must be an integer.");
        }
        return result;
    }

    private static string[] RequiredStringArray(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out JsonElement property))
        {
            throw new InvalidOperationException($"Restore journal field {name} is required.");
        }
        return ParseStringArray(property, name);
    }

    private static string[] ParseStringArray(JsonElement value, string name)
    {
        if (value.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException($"Restore journal field {name} must be an array.");
        }
        List<string> result = [];
        foreach (JsonElement item in value.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(item.GetString()))
            {
                throw new InvalidOperationException($"Restore journal field {name} contains an invalid value.");
            }
            result.Add(item.GetString()!);
        }
        return [.. result];
    }

    private static string? TryAbsoluteNestedPath(JsonElement root, string objectName, string propertyName)
    {
        if (!root.TryGetProperty(objectName, out JsonElement nested)
            || nested.ValueKind != JsonValueKind.Object
            || !nested.TryGetProperty(propertyName, out JsonElement property)
            || property.ValueKind != JsonValueKind.String)
        {
            return null;
        }
        string? value = property.GetString();
        return !string.IsNullOrWhiteSpace(value) && Path.IsPathFullyQualified(value)
            ? Path.GetFullPath(value)
            : null;
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;
}

internal static class RestoreJournalDurability
{
    internal static void SyncDirectory(string directory)
    {
        if (OperatingSystem.IsWindows())
        {
            nint handle = CreateFileW(
                directory,
                0x80000000,
                0x00000001 | 0x00000002 | 0x00000004,
                nint.Zero,
                3,
                0x02000000 | 0x80000000,
                nint.Zero);
            if (handle == new nint(-1))
            {
                int errorCode = Marshal.GetLastPInvokeError();
                if (IsUnsupportedWindowsDirectorySyncError(errorCode))
                {
                    return;
                }
                throw new IOException(
                    "Unable to open Restore journal directory for durability sync.",
                    new System.ComponentModel.Win32Exception(errorCode));
            }
            try
            {
                if (!FlushFileBuffers(handle))
                {
                    int errorCode = Marshal.GetLastPInvokeError();
                    if (!IsUnsupportedWindowsDirectorySyncError(errorCode))
                    {
                        throw new IOException(
                            "Unable to sync Restore journal directory.",
                            new System.ComponentModel.Win32Exception(errorCode));
                    }
                }
            }
            finally
            {
                _ = CloseHandle(handle);
            }
            return;
        }

        int fd = open(directory, 0);
        if (fd < 0)
        {
            throw new IOException($"Unable to open Restore journal directory for durability sync: {directory}");
        }
        try
        {
            if (fsync(fd) != 0)
            {
                throw new IOException($"Unable to sync Restore journal directory: {directory}");
            }
        }
        finally
        {
            _ = close(fd);
        }
    }

    private static bool IsUnsupportedWindowsDirectorySyncError(int errorCode) => errorCode is
        1     // ERROR_INVALID_FUNCTION / EINVAL
        or 5  // ERROR_ACCESS_DENIED / EACCES or EPERM
        or 6  // ERROR_INVALID_HANDLE / EBADF
        or 50 // ERROR_NOT_SUPPORTED / ENOTSUP
        or 87 // ERROR_INVALID_PARAMETER / EINVAL
        or 1314; // ERROR_PRIVILEGE_NOT_HELD / EPERM

    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern nint CreateFileW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        nint securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        nint templateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FlushFileBuffers(nint handle);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseHandle(nint handle);

    [DllImport("libc", SetLastError = true)]
    private static extern int open([MarshalAs(UnmanagedType.LPUTF8Str)] string path, int flags);

    [DllImport("libc", SetLastError = true)]
    private static extern int fsync(int fd);

    [DllImport("libc", SetLastError = true)]
    private static extern int close(int fd);
}
