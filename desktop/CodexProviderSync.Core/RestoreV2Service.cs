using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CodexProviderSync.Core;

internal sealed record RestoreSnapshotLocator(
    [property: JsonPropertyName("backupId")] string BackupId,
    [property: JsonPropertyName("backupDir")] string BackupDir);

internal sealed class RestoreSnapshotManifestFile
{
    public int SchemaVersion { get; init; }
    public int ProtocolVersion { get; init; }
    public string OperationKind { get; init; } = string.Empty;
    public string OperationId { get; init; } = string.Empty;
    public DateTimeOffset CreatedAt { get; init; }
    public required RestoreBackupIdentity SourceBackup { get; init; }
    public required RestoreSnapshotLocator PreRestoreSnapshot { get; init; }
    public required RestoreStorageIdentity Storage { get; init; }
    public IReadOnlyList<string> RequiredTargetKinds { get; init; } = [];
    public IReadOnlyList<string> ResolvesOperationIds { get; init; } = [];
    public IReadOnlyList<RestoreJournalTarget> Targets { get; init; } = [];
}

internal sealed record RestorePreSnapshot(
    string BackupId,
    string BackupDirectory,
    string Revision,
    string ManifestSha256,
    RestoreSnapshotManifestFile Manifest);

internal sealed class RestoreV2Service
{
    internal const string SnapshotManifestFileName = "restore-snapshot.v2.json";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private static readonly Regex ModelFieldRegex = new(
        "\\\"model\\\"\\s*:\\s*(?<value>\\\"(?:\\\\.|[^\\\"\\\\])*\\\")",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex TurnContextTypeRegex = new(
        "\\\"type\\\"\\s*:\\s*\\\"turn_context\\\"",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly BackupService _backupService;
    private readonly SessionRolloutService _sessionRolloutService;
    private readonly SqliteStateService _sqliteStateService;

    internal RestoreV2Service(
        BackupService backupService,
        SessionRolloutService sessionRolloutService,
        SqliteStateService sqliteStateService)
    {
        _backupService = backupService;
        _sessionRolloutService = sessionRolloutService;
        _sqliteStateService = sqliteStateService;
    }

    internal Func<string, string?, int, Task>? FaultInjector { get; set; }

    internal static async Task<RestoreBackupIdentity> CaptureSourceIdentityAsync(
        string backupDir,
        CancellationToken cancellationToken = default)
    {
        string root = Path.GetFullPath(backupDir);
        if (!Directory.Exists(root))
        {
            throw new InvalidOperationException("The selected managed backup is unavailable.");
        }
        List<(string Path, string Sha256)> files = [];
        await CollectIdentityFilesAsync(root, root, files, cancellationToken);
        files.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Path, right.Path));
        using MemoryStream json = new();
        using (Utf8JsonWriter writer = new(json, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            writer.WriteStartArray();
            foreach ((string relativePath, string sha256) in files)
            {
                writer.WriteStartObject();
                writer.WriteString("path", relativePath);
                writer.WriteString("sha256", sha256);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return new RestoreBackupIdentity(
            Path.GetFileName(root),
            root,
            Sha256Base64Url(json.ToArray()));
    }

    internal async Task<RestoreResult> ExecuteAsync(
        RestoreBackupPlan plan,
        RestoreBackupIdentity sourceBackup,
        StateDbLockResource? stateDbResource,
        IReadOnlyList<string> resolvesOperationIds,
        CancellationToken cancellationToken = default)
    {
        string operationId = Guid.NewGuid().ToString("D");
        RestorePreSnapshot snapshot = await CreatePreSnapshotAsync(
            operationId,
            plan,
            sourceBackup,
            stateDbResource,
            resolvesOperationIds,
            cancellationToken);
        RestoreJournalPrepared prepared = new(
            sourceBackup,
            new RestorePreSnapshotIdentity(
                snapshot.BackupId,
                snapshot.BackupDirectory,
                snapshot.Revision,
                snapshot.ManifestSha256),
            snapshot.Manifest.Storage,
            snapshot.Manifest.RequiredTargetKinds,
            snapshot.Manifest.ResolvesOperationIds,
            snapshot.Manifest.Targets);

        RestoreJournal journal;
        try
        {
            journal = await RestoreJournal.CreateAsync(
                snapshot.BackupDirectory,
                operationId,
                prepared,
                cancellationToken);
        }
        catch (Exception error)
        {
            TryDeleteDirectory(snapshot.BackupDirectory);
            throw new InvalidOperationException(
                "Unable to persist the Restore journal before mutation.",
                error);
        }

        Dictionary<string, RestoreJournalTarget> targetEvidence = snapshot.Manifest.Targets
            .ToDictionary(static target => TargetKey(target.Kind, target.TargetPath), StringComparer.Ordinal);
        bool mutationMayHaveOccurred = false;
        try
        {
            await InvokeFaultAsync("after_restore_prepared_before_applying", null, 0);
            cancellationToken.ThrowIfCancellationRequested();
            await journal.ApplyingAsync(cancellationToken);
            int completedCount = 0;
            foreach (RestoreBackupTarget target in plan.Targets)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!targetEvidence.TryGetValue(TargetKey(target.Kind, target.TargetPath), out RestoreJournalTarget? evidence))
                {
                    throw new InvalidOperationException("Restore attempted an undeclared target.");
                }
                ValidatePhysicalTargetBoundary(
                    evidence.Kind,
                    evidence.TargetPath,
                    snapshot.Manifest.Storage,
                    plan.Storage);
                await journal.TargetIntentAsync(evidence.Id, cancellationToken);
                mutationMayHaveOccurred = true;
                await InvokeFaultAsync(
                    "after_restore_target_intent_before_write",
                    target.TargetPath,
                    completedCount);
                ValidatePhysicalTargetBoundary(
                    evidence.Kind,
                    evidence.TargetPath,
                    snapshot.Manifest.Storage,
                    plan.Storage);
                await _backupService.ApplyRestoreTargetAsync(plan, target, CancellationToken.None);
                await InvokeFaultAsync(
                    "after_restore_target_write_before_complete",
                    target.TargetPath,
                    completedCount);
                ValidatePhysicalTargetBoundary(
                    evidence.Kind,
                    evidence.TargetPath,
                    snapshot.Manifest.Storage,
                    plan.Storage);
                RestoreDigest actual = await DigestTargetAsync(
                    evidence,
                    snapshot.BackupDirectory,
                    plan.Storage,
                    CancellationToken.None);
                if (!SameDigest(actual, evidence.ExpectedPost))
                {
                    throw new InvalidOperationException(
                        $"Restore target post-write digest verification failed for {evidence.Kind}.");
                }
                await journal.TargetCompletedAsync(evidence.Id, actual.Digest, CancellationToken.None);
                completedCount++;
                await InvokeFaultAsync(
                    "after_restore_target_complete",
                    target.TargetPath,
                    completedCount);
            }

            string postManifestSha256 = await VerifyManifestTargetsAsync(
                snapshot.Manifest,
                expectedPre: false,
                snapshot.BackupDirectory,
                plan.Storage,
                CancellationToken.None);
            await InvokeFaultAsync(
                "after_restore_targets_verify_before_committing",
                null,
                plan.Targets.Count);
            await journal.CommittingAsync(postManifestSha256, CancellationToken.None);
            await InvokeFaultAsync(
                "after_restore_committing_before_committed_pending_ack",
                null,
                plan.Targets.Count);
            await journal.CommittedPendingAckAsync(postManifestSha256, CancellationToken.None);
            await InvokeFaultAsync(
                "after_restore_committed_pending_ack_before_completed",
                null,
                plan.Targets.Count);
            RestoreJournalInfo current = await RestoreJournalService.ReadInfoAsync(
                journal.FilePath,
                CancellationToken.None);
            RestoreJournalInfo completed = await AcknowledgeCommittedAsync(
                current,
                plan.Storage,
                stateDbResource,
                CancellationToken.None);
            return BuildResult(
                plan,
                operationId,
                snapshot.BackupId,
                completed.State,
                snapshot.Manifest.ResolvesOperationIds,
                commitAcknowledgementRecovered: false);
        }
        catch (Exception originalError)
        {
            RestoreJournalInfo current;
            try
            {
                current = await RestoreJournalService.ReadInfoAsync(journal.FilePath, CancellationToken.None);
            }
            catch (Exception journalError)
            {
                throw RecoveryRequired(
                    "Restore journal cannot be read after an interrupted operation.",
                    snapshot,
                    sourceBackup,
                    journalError);
            }

            if (current.State == "completed" && !current.InvalidTail)
            {
                return BuildResult(
                    plan,
                    operationId,
                    snapshot.BackupId,
                    "completed",
                    snapshot.Manifest.ResolvesOperationIds,
                    commitAcknowledgementRecovered: false);
            }
            if (current.State == "committed-pending-ack" && !current.InvalidTail)
            {
                try
                {
                    RestoreJournalInfo completed = await AcknowledgeCommittedAsync(
                        current,
                        plan.Storage,
                        stateDbResource,
                        CancellationToken.None);
                    return BuildResult(
                        plan,
                        operationId,
                        snapshot.BackupId,
                        completed.State,
                        snapshot.Manifest.ResolvesOperationIds,
                        commitAcknowledgementRecovered: true);
                }
                catch (Exception acknowledgementError)
                {
                    await TryMarkRecoveryRequiredAsync(current, "commit-ack-unverifiable");
                    throw RecoveryRequired(
                        "Restore committed, but its final acknowledgement is unverifiable.",
                        snapshot,
                        sourceBackup,
                        acknowledgementError);
                }
            }
            if (current.InvalidTail || current.Prepared is null)
            {
                throw RecoveryRequired(
                    "Restore journal evidence is incomplete; compensation was not attempted.",
                    snapshot,
                    sourceBackup,
                    originalError);
            }

            try
            {
                RestoreJournal writer = RestoreJournal.Reopen(current);
                if (current.State != "rollback-pending")
                {
                    await writer.RollbackPendingAsync(ErrorCode(originalError), CancellationToken.None);
                }
                RestoreJournalInfo rollback = await RestoreJournalService.ReadInfoAsync(
                    writer.FilePath,
                    CancellationToken.None);
                await CompensateAsync(
                    rollback,
                    writer,
                    plan.Storage,
                    stateDbResource,
                    mutationMayHaveOccurred,
                    CancellationToken.None);
                await writer.RolledBackAsync(CancellationToken.None);
                RestoreJournalInfo terminal = await RestoreJournalService.ReadInfoAsync(
                    writer.FilePath,
                    CancellationToken.None);
                if (terminal.InvalidTail || terminal.State != "rolled-back")
                {
                    throw new InvalidOperationException("Restore rollback terminal state did not persist.");
                }
            }
            catch (Exception rollbackError)
            {
                RestoreJournalInfo latest = await SafeReadAsync(journal.FilePath, current);
                await TryMarkRecoveryRequiredAsync(latest, "rollback-unverifiable");
                throw RecoveryRequired(
                    "Restore failed and its compensation could not be verified.",
                    snapshot,
                    sourceBackup,
                    rollbackError);
            }

            if (!mutationMayHaveOccurred)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalError).Throw();
            }
            throw new SyncTransactionException(
                originalError,
                [],
                snapshot.BackupDirectory,
                plan.Targets.Select(static target => target.TargetPath).ToArray(),
                [],
                rollbackStatus: "complete",
                recoveryRequired: false);
        }
    }

    internal async Task<RestoreJournalInfo> AcknowledgePendingAsync(
        RestoreJournalInfo journal,
        CodexStorageLayout storage,
        StateDbLockResource? stateDbResource,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await AcknowledgeCommittedAsync(journal, storage, stateDbResource, cancellationToken);
        }
        catch
        {
            await TryMarkRecoveryRequiredAsync(journal, "commit-ack-unverifiable");
            string[] evidenceDirectories =
            [
                journal.SnapshotDir,
                .. journal.Prepared is null
                    ? []
                    : new[] { journal.Prepared.SourceBackup.BackupDir }
            ];
            throw new RecoveryRequiredException(
                "Restore committed, but its final acknowledgement evidence is unverifiable.",
                evidenceDirectories);
        }
    }

    private async Task<RestorePreSnapshot> CreatePreSnapshotAsync(
        string operationId,
        RestoreBackupPlan plan,
        RestoreBackupIdentity sourceBackup,
        StateDbLockResource? stateDbResource,
        IReadOnlyList<string> resolvesOperationIds,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string codexHomePhysical = ResolveStablePhysicalDirectory(plan.Storage.CodexHome);
        RestoreStorageIdentity boundaryStorage = new(
            Path.GetFullPath(plan.Storage.CodexHome),
            codexHomePhysical,
            Path.GetFullPath(plan.Storage.SqliteHome),
            stateDbResource?.ResourceKey,
            plan.StateDbTargetPath is null ? null : Path.GetFullPath(plan.StateDbTargetPath));
        string backupRoot = AppConstants.DefaultBackupRoot(plan.Storage.CodexHome);
        Directory.CreateDirectory(backupRoot);
        string backupId = $"restore-v2-{operationId}";
        string snapshotDir = Path.Combine(backupRoot, backupId);
        Directory.CreateDirectory(snapshotDir);
        try
        {
            List<SessionBackupManifestEntry> rolloutEntries = [];
            List<RestoreJournalTarget> targets = [];
            foreach (RestoreBackupTarget sourceTarget in plan.Targets)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ValidatePhysicalTargetBoundary(
                    sourceTarget.Kind,
                    sourceTarget.TargetPath,
                    boundaryStorage,
                    plan.Storage);
                string id = TargetId(sourceTarget.Kind, sourceTarget.TargetPath);
                RestoreDigest pre;
                string? snapshotPath = null;
                int? snapshotEntryIndex = null;
                if (sourceTarget.Kind == "rollout")
                {
                    SessionBackupManifestEntry entry = await CaptureRolloutEntryAsync(
                        sourceTarget.TargetPath,
                        cancellationToken);
                    pre = DigestRolloutEntry(entry);
                    snapshotEntryIndex = rolloutEntries.Count;
                    rolloutEntries.Add(entry);
                }
                else if (sourceTarget.Kind == "sqlite")
                {
                    pre = await DigestSqliteAsync(
                        sourceTarget.TargetPath,
                        snapshotDir,
                        plan.Storage,
                        cancellationToken);
                    if (pre.Present)
                    {
                        snapshotPath = Path.Combine("db", "sqlite-home", AppConstants.DbFileBasename);
                        string destination = Path.Combine(snapshotDir, snapshotPath);
                        CodexStorageLayout sourceStorage = StorageForDatabase(
                            plan.Storage,
                            sourceTarget.TargetPath,
                            "restore-v2-pre-snapshot");
                        SqliteOnlineBackupResult backup = await _sqliteStateService.CreateSqliteOnlineBackupAsync(
                            sourceStorage,
                            destination);
                        if (!backup.DatabasePresent)
                        {
                            throw new InvalidOperationException(
                                "The State DB disappeared during the Restore pre-snapshot.");
                        }
                        RestoreDigest copied = await DigestSqliteAsync(
                            destination,
                            snapshotDir,
                            plan.Storage,
                            cancellationToken);
                        if (!SameDigest(pre, copied))
                        {
                            throw new InvalidOperationException(
                                "The Restore pre-snapshot SQLite digest did not verify.");
                        }
                    }
                }
                else
                {
                    pre = await DigestFileAsync(sourceTarget.TargetPath, cancellationToken);
                    if (pre.Present)
                    {
                        snapshotPath = SnapshotRelativePath(sourceTarget);
                        string destination = Path.Combine(snapshotDir, snapshotPath);
                        if (!await AtomicFile.CopyAsync(
                            sourceTarget.TargetPath,
                            destination,
                            overwrite: false,
                            cancellationToken))
                        {
                            throw new InvalidOperationException(
                                "A Restore target disappeared during pre-snapshot copy.");
                        }
                        RestoreDigest copied = await DigestFileAsync(destination, cancellationToken);
                        if (!SameDigest(pre, copied))
                        {
                            throw new InvalidOperationException(
                                "A Restore pre-snapshot file did not match its source digest.");
                        }
                    }
                }
                await InvokeFaultAsync(
                    "after_restore_pre_snapshot_target_before_hash",
                    sourceTarget.TargetPath,
                    targets.Count);
                RestoreDigest expectedPost = await ExpectedPostDigestAsync(
                    sourceTarget,
                    pre,
                    snapshotDir,
                    plan.Storage,
                    cancellationToken);
                targets.Add(new RestoreJournalTarget(
                    id,
                    sourceTarget.Kind,
                    Path.GetFullPath(sourceTarget.TargetPath),
                    pre,
                    expectedPost,
                    snapshotPath?.Replace(Path.DirectorySeparatorChar, '/'),
                    snapshotEntryIndex));
            }

            DateTimeOffset createdAt = DateTimeOffset.UtcNow;
            SessionBackupManifest sessionManifest = new()
            {
                Version = 2,
                Namespace = AppConstants.BackupNamespace,
                CodexHome = plan.Storage.CodexHome,
                TargetProvider = plan.Metadata.TargetProvider,
                CreatedAt = createdAt,
                Files = rolloutEntries
            };
            await AtomicFile.WriteAllTextAsync(
                Path.Combine(snapshotDir, "session-meta-backup.json"),
                JsonSerializer.Serialize(sessionManifest, JsonOptions),
                cancellationToken);

            RestoreStorageIdentity storage = new(
                Path.GetFullPath(plan.Storage.CodexHome),
                codexHomePhysical,
                Path.GetFullPath(plan.Storage.SqliteHome),
                stateDbResource?.ResourceKey,
                plan.StateDbTargetPath is null ? null : Path.GetFullPath(plan.StateDbTargetPath));
            RestoreSnapshotManifestFile manifest = new()
            {
                SchemaVersion = 2,
                ProtocolVersion = 2,
                OperationKind = "restore",
                OperationId = operationId,
                CreatedAt = createdAt,
                SourceBackup = sourceBackup,
                PreRestoreSnapshot = new RestoreSnapshotLocator(backupId, Path.GetFullPath(snapshotDir)),
                Storage = storage,
                RequiredTargetKinds = targets
                    .Select(static target => target.Kind)
                    .Distinct(StringComparer.Ordinal)
                    .Order(StringComparer.Ordinal)
                    .ToArray(),
                ResolvesOperationIds = resolvesOperationIds
                    .Where(static value => !string.IsNullOrWhiteSpace(value))
                    .Distinct(StringComparer.Ordinal)
                    .Order(StringComparer.Ordinal)
                    .ToArray(),
                Targets = targets
            };
            string manifestText = JsonSerializer.Serialize(manifest, JsonOptions) + "\n";
            string manifestSha256 = Sha256Base64Url(Encoding.UTF8.GetBytes(manifestText));
            await AtomicFile.WriteAllTextAsync(
                Path.Combine(snapshotDir, SnapshotManifestFileName),
                manifestText,
                cancellationToken);
            await InvokeFaultAsync(
                "after_restore_pre_snapshot_manifest_before_prepared",
                null,
                targets.Count);

            Dictionary<string, bool> globalStateFiles = new(StringComparer.Ordinal)
            {
                [AppConstants.GlobalStateFileBasename] = File.Exists(
                    Path.Combine(snapshotDir, AppConstants.GlobalStateFileBasename)),
                [AppConstants.GlobalStateBackupFileBasename] = File.Exists(
                    Path.Combine(snapshotDir, AppConstants.GlobalStateBackupFileBasename))
            };
            Dictionary<string, object?> metadata = new()
            {
                ["version"] = 2,
                ["namespace"] = AppConstants.BackupNamespace,
                ["backupKind"] = "restore-pre-snapshot",
                ["restoreOperationId"] = operationId,
                ["codexHome"] = plan.Storage.CodexHome,
                ["sqliteHome"] = plan.StateDbTargetPath is null
                    ? plan.Storage.SqliteHome
                    : Path.GetDirectoryName(plan.StateDbTargetPath),
                ["targetProvider"] = plan.Metadata.TargetProvider,
                ["createdAt"] = createdAt,
                ["dbFiles"] = Array.Empty<string>(),
                ["sqliteDbFiles"] = targets.Any(static target => target.Kind == "sqlite" && target.Pre.Present)
                    ? new[] { AppConstants.DbFileBasename }
                    : Array.Empty<string>(),
                ["changedSessionFiles"] = rolloutEntries.Count,
                ["globalStateFiles"] = globalStateFiles,
                ["restoreSnapshotManifestSha256"] = manifestSha256
            };
            await AtomicFile.WriteAllTextAsync(
                Path.Combine(snapshotDir, "metadata.json"),
                JsonSerializer.Serialize(metadata, JsonOptions),
                cancellationToken);
            return new RestorePreSnapshot(
                backupId,
                Path.GetFullPath(snapshotDir),
                manifestSha256,
                manifestSha256,
                manifest);
        }
        catch
        {
            TryDeleteDirectory(snapshotDir);
            throw;
        }
    }

    private async Task<RestoreJournalInfo> AcknowledgeCommittedAsync(
        RestoreJournalInfo journal,
        CodexStorageLayout storage,
        StateDbLockResource? stateDbResource,
        CancellationToken cancellationToken)
    {
        if (journal.InvalidTail
            || journal.State != "committed-pending-ack"
            || journal.Prepared is null)
        {
            throw new InvalidOperationException(
                "Restore commit acknowledgement evidence is incomplete.");
        }
        RestoreSnapshotManifestFile manifest = await ReadVerifiedSnapshotAsync(
            journal,
            cancellationToken);
        ValidateManifestTargetBoundaries(journal, manifest, storage);
        if (manifest.RequiredTargetKinds.Contains("sqlite", StringComparer.Ordinal))
        {
            if (stateDbResource is null
                || manifest.Storage.StateDbResourceKey != stateDbResource.ResourceKey
                || string.IsNullOrWhiteSpace(manifest.Storage.TargetStateDbPath))
            {
                throw new InvalidOperationException(
                    "Restore State DB identity changed before commit acknowledgement.");
            }
            StateDbLockResource current = await StateDbLockResource.ResolveAsync(
                manifest.Storage.TargetStateDbPath,
                cancellationToken);
            if (current.ResourceKey != stateDbResource.ResourceKey)
            {
                throw new InvalidOperationException(
                    "Restore State DB physical identity changed before commit acknowledgement.");
            }
        }
        string verifiedManifest = await VerifyManifestTargetsAsync(
            manifest,
            expectedPre: false,
            journal.SnapshotDir,
            storage,
            cancellationToken);
        RestoreJournalEvent? committedEvent = journal.Events
            .LastOrDefault(static item => item.State == "committed-pending-ack");
        if (committedEvent?.PostManifestSha256 != verifiedManifest)
        {
            throw new InvalidOperationException(
                "Restore post-commit manifest acknowledgement failed.");
        }

        await FileTransactionJournal.MarkBackupRolledBackAsync(
            journal.Prepared.SourceBackup.BackupDir,
            storage.CodexHome,
            ReadSourceTargetProvider(journal.Prepared.SourceBackup.BackupDir));
        await InvokeFaultAsync(
            "after_restore_source_journal_ack_before_completed",
            null,
            manifest.Targets.Count);
        RestoreJournal writer = RestoreJournal.Reopen(journal);
        await writer.CompletedAsync(CancellationToken.None);
        RestoreJournalInfo completed = await RestoreJournalService.ReadInfoAsync(
            writer.FilePath,
            CancellationToken.None);
        if (completed.InvalidTail || completed.State != "completed")
        {
            throw new InvalidOperationException(
                "Restore completed acknowledgement did not persist.");
        }
        try
        {
            await _backupService.RefreshMetadataInventoryAsync(
                journal.Prepared.PreRestoreSnapshot.BackupDir);
        }
        catch
        {
            // Inventory is bookkeeping; the verified Restore is authoritative.
        }
        return completed;
    }

    private async Task CompensateAsync(
        RestoreJournalInfo journal,
        RestoreJournal writer,
        CodexStorageLayout storage,
        StateDbLockResource? stateDbResource,
        bool mutateTargets,
        CancellationToken cancellationToken)
    {
        RestoreSnapshotManifestFile manifest = await ReadVerifiedSnapshotAsync(journal, cancellationToken);
        ValidateManifestTargetBoundaries(journal, manifest, storage);
        foreach (RestoreJournalTarget target in manifest.Targets.Reverse())
        {
            if (target.Kind == "sqlite")
            {
                if (stateDbResource is null
                    || manifest.Storage.StateDbResourceKey != stateDbResource.ResourceKey)
                {
                    throw new InvalidOperationException(
                        "Restore compensation has no verified State DB lock identity.");
                }
                StateDbLockResource current = await StateDbLockResource.ResolveAsync(
                    target.TargetPath,
                    cancellationToken);
                if (current.ResourceKey != stateDbResource.ResourceKey)
                {
                    throw new InvalidOperationException(
                        "Restore State DB physical identity changed before compensation.");
                }
            }
            await InvokeFaultAsync(
                "after_restore_rollback_pending_before_target",
                target.TargetPath,
                0);
            ValidatePhysicalTargetBoundary(
                target.Kind,
                target.TargetPath,
                manifest.Storage,
                storage);
            if (mutateTargets)
            {
                await RestoreTargetFromSnapshotAsync(target, manifest, storage, cancellationToken);
            }
            RestoreDigest actual = await DigestTargetAsync(
                target,
                journal.SnapshotDir,
                storage,
                cancellationToken);
            if (!SameDigest(actual, target.Pre))
            {
                throw new InvalidOperationException(
                    $"Restore compensation digest failed for {target.Kind}.");
            }
            await writer.TargetCompensatedAsync(target.Id, actual.Digest, CancellationToken.None);
            await InvokeFaultAsync(
                "after_restore_compensation_verify_before_next",
                target.TargetPath,
                0);
        }
        _ = await VerifyManifestTargetsAsync(
            manifest,
            expectedPre: true,
            journal.SnapshotDir,
            storage,
            cancellationToken);
    }

    private async Task RestoreTargetFromSnapshotAsync(
        RestoreJournalTarget target,
        RestoreSnapshotManifestFile manifest,
        CodexStorageLayout storage,
        CancellationToken cancellationToken)
    {
        ValidatePhysicalTargetBoundary(
            target.Kind,
            target.TargetPath,
            manifest.Storage,
            storage);
        string snapshotDir = Path.GetFullPath(manifest.PreRestoreSnapshot.BackupDir);
        if (target.Kind == "rollout")
        {
            SessionBackupManifest sessionManifest = JsonSerializer.Deserialize<SessionBackupManifest>(
                await File.ReadAllTextAsync(
                    Path.Combine(snapshotDir, "session-meta-backup.json"),
                    cancellationToken),
                JsonOptions) ?? throw new InvalidOperationException(
                    "Restore snapshot session manifest is invalid.");
            int index = target.SnapshotEntryIndex
                ?? throw new InvalidOperationException("Restore snapshot rollout index is missing.");
            if (index < 0 || index >= sessionManifest.Files.Count)
            {
                throw new InvalidOperationException("Restore snapshot rollout entry is missing.");
            }
            SessionBackupManifestEntry entry = sessionManifest.Files[index];
            if (!PathsEqual(entry.Path, target.TargetPath))
            {
                throw new InvalidOperationException("Restore snapshot rollout entry is mismatched.");
            }
            ValidatePhysicalTargetBoundary(
                target.Kind,
                target.TargetPath,
                manifest.Storage,
                storage);
            await _sessionRolloutService.RestoreSessionChangesAsync([entry]);
            return;
        }
        if (target.Kind == "sqlite")
        {
            if (target.Pre.Present)
            {
                await _sqliteStateService.RestoreSqliteOnlineBackupAsync(
                    SafeSnapshotPath(snapshotDir, target.SnapshotPath),
                    target.TargetPath);
            }
            else
            {
                if (File.Exists(target.TargetPath + "-wal") || File.Exists(target.TargetPath + "-shm"))
                {
                    throw new InvalidOperationException(
                        "Cannot remove a newly created State DB while SQLite sidecars are present.");
                }
                if (File.Exists(target.TargetPath))
                {
                    File.Delete(target.TargetPath);
                }
            }
            return;
        }
        if (target.Pre.Present)
        {
            await AtomicFile.CopyAsync(
                SafeSnapshotPath(snapshotDir, target.SnapshotPath),
                target.TargetPath,
                overwrite: true,
                cancellationToken);
        }
        else if (File.Exists(target.TargetPath))
        {
            File.Delete(target.TargetPath);
        }
    }

    private static async Task<RestoreSnapshotManifestFile> ReadVerifiedSnapshotAsync(
        RestoreJournalInfo journal,
        CancellationToken cancellationToken)
    {
        RestoreJournalPrepared prepared = journal.Prepared
            ?? throw new InvalidOperationException("Restore journal prepared evidence is missing.");
        if (!PathsEqual(journal.SnapshotDir, prepared.PreRestoreSnapshot.BackupDir))
        {
            throw new InvalidOperationException("Restore snapshot directory does not match its journal.");
        }
        string manifestPath = Path.Combine(
            prepared.PreRestoreSnapshot.BackupDir,
            SnapshotManifestFileName);
        string text = await File.ReadAllTextAsync(manifestPath, cancellationToken);
        string digest = Sha256Base64Url(Encoding.UTF8.GetBytes(text));
        if (digest != prepared.PreRestoreSnapshot.ManifestSha256)
        {
            throw new InvalidOperationException("Restore snapshot manifest verification failed.");
        }
        RestoreSnapshotManifestFile manifest = JsonSerializer.Deserialize<RestoreSnapshotManifestFile>(
            text,
            JsonOptions) ?? throw new InvalidOperationException(
                "Restore snapshot manifest is invalid.");
        if (manifest.SchemaVersion != 2
            || manifest.ProtocolVersion != 2
            || manifest.OperationKind != "restore"
            || manifest.OperationId != journal.OperationId
            || manifest.PreRestoreSnapshot.BackupId != prepared.PreRestoreSnapshot.BackupId
            || !PathsEqual(manifest.PreRestoreSnapshot.BackupDir, journal.SnapshotDir)
            || !ManifestMatchesPrepared(manifest, prepared))
        {
            throw new InvalidOperationException("Restore snapshot identity verification failed.");
        }
        return manifest;
    }

    private static bool ManifestMatchesPrepared(
        RestoreSnapshotManifestFile manifest,
        RestoreJournalPrepared prepared)
    {
        if (manifest.SourceBackup != prepared.SourceBackup
            || manifest.Storage != prepared.Storage
            || !manifest.RequiredTargetKinds.SequenceEqual(prepared.RequiredTargetKinds)
            || !manifest.ResolvesOperationIds.SequenceEqual(prepared.ResolvesOperationIds)
            || manifest.Targets.Count != prepared.Targets.Count)
        {
            return false;
        }
        return manifest.Targets.Zip(prepared.Targets).All(pair => pair.First == pair.Second);
    }

    private static string ResolveStablePhysicalDirectory(string directory)
    {
        try
        {
            string lexical = Path.GetFullPath(directory);
            string first = StateDbLockResource.ResolveExistingPhysicalPath(lexical, directory: true);
            string second = StateDbLockResource.ResolveExistingPhysicalPath(lexical, directory: true);
            if (!PathsEqual(first, second))
            {
                throw new InvalidOperationException(
                    "Restore physical directory identity changed while it was resolved.");
            }
            return Path.GetFullPath(first);
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception error) when (error is IOException
            or UnauthorizedAccessException
            or System.ComponentModel.Win32Exception
            or ArgumentException
            or NotSupportedException
            or System.Security.SecurityException)
        {
            throw new InvalidOperationException(
                "A Restore physical directory identity cannot be verified.",
                error);
        }
    }

    private static string ValidateRestoreHomePhysicalIdentity(
        RestoreStorageIdentity manifestStorage,
        CodexStorageLayout runtimeStorage)
    {
        if (!Path.IsPathFullyQualified(manifestStorage.CodexHome)
            || !Path.IsPathFullyQualified(manifestStorage.CodexHomePhysical))
        {
            throw new InvalidOperationException(
                "Restore Codex Home physical identity evidence is missing.");
        }
        string manifestPhysical = ResolveStablePhysicalDirectory(manifestStorage.CodexHome);
        string runtimePhysical = ResolveStablePhysicalDirectory(runtimeStorage.CodexHome);
        if (!PathsEqual(manifestPhysical, manifestStorage.CodexHomePhysical)
            || !PathsEqual(runtimePhysical, manifestStorage.CodexHomePhysical))
        {
            throw new InvalidOperationException(
                "Restore Codex Home physical identity changed.");
        }
        return manifestPhysical;
    }

    internal static bool JournalMatchesCurrentPhysicalHome(
        RestoreJournalInfo journal,
        CodexStorageLayout runtimeStorage)
    {
        if (journal.Prepared is null)
        {
            return false;
        }
        try
        {
            _ = ValidateRestoreHomePhysicalIdentity(journal.Prepared.Storage, runtimeStorage);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private static void ValidatePhysicalTargetBoundary(
        string kind,
        string targetPath,
        RestoreStorageIdentity manifestStorage,
        CodexStorageLayout runtimeStorage)
    {
        if (kind == "sqlite")
        {
            return;
        }
        string physicalHome = ValidateRestoreHomePhysicalIdentity(manifestStorage, runtimeStorage);
        string lexicalTarget = Path.GetFullPath(targetPath);
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        string[] segments;
        if (kind == "config" || kind == "globalState")
        {
            string fileName = Path.GetFileName(lexicalTarget);
            bool validFileName = kind == "config"
                ? string.Equals(fileName, "config.toml", comparison)
                : string.Equals(fileName, AppConstants.GlobalStateFileBasename, comparison)
                    || string.Equals(fileName, AppConstants.GlobalStateBackupFileBasename, comparison);
            string parentPhysical = ResolveStablePhysicalDirectory(
                Path.GetDirectoryName(lexicalTarget)
                    ?? throw new InvalidOperationException("Restore target parent is missing."));
            if (!validFileName || !PathsEqual(parentPhysical, physicalHome))
            {
                throw new InvalidOperationException(
                    "Restore target is outside its kind-specific storage boundary.");
            }
            segments = [fileName];
        }
        else if (kind == "rollout")
        {
            if (!Regex.IsMatch(
                    Path.GetFileName(lexicalTarget),
                    "^rollout-.*\\.jsonl$",
                    RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            {
                throw new InvalidOperationException(
                    "Restore target is outside its kind-specific storage boundary.");
            }
            string? rawRoot = Path.GetDirectoryName(lexicalTarget);
            while (rawRoot is not null
                && !string.Equals(Path.GetFileName(rawRoot), "sessions", comparison)
                && !string.Equals(Path.GetFileName(rawRoot), "archived_sessions", comparison))
            {
                string? parent = Path.GetDirectoryName(rawRoot);
                if (parent is null || PathsEqual(parent, rawRoot))
                {
                    rawRoot = null;
                    break;
                }
                rawRoot = parent;
            }
            if (rawRoot is null
                || !PathsEqual(
                    ResolveStablePhysicalDirectory(
                        Path.GetDirectoryName(rawRoot)
                            ?? throw new InvalidOperationException("Restore rollout root parent is missing.")),
                    physicalHome))
            {
                throw new InvalidOperationException(
                    "Restore target is outside its kind-specific storage boundary.");
            }
            string nested = Path.GetRelativePath(rawRoot, lexicalTarget);
            if (string.IsNullOrWhiteSpace(nested)
                || nested == ".."
                || nested.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || Path.IsPathRooted(nested))
            {
                throw new InvalidOperationException(
                    "Restore target is outside its kind-specific storage boundary.");
            }
            segments = [
                Path.GetFileName(rawRoot),
                .. nested.Split(
                    [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                    StringSplitOptions.RemoveEmptyEntries)
            ];
        }
        else
        {
            throw new InvalidOperationException(
                "Restore target is outside its kind-specific storage boundary.");
        }

        string current = physicalHome;
        for (int index = 0; index < segments.Length; index++)
        {
            current = Path.Combine(current, segments[index]);
            FileAttributes attributes;
            try
            {
                attributes = File.GetAttributes(current);
            }
            catch (Exception error) when (error is FileNotFoundException or DirectoryNotFoundException)
            {
                bool isLast = index == segments.Length - 1;
                if (isLast && (kind == "config" || kind == "globalState"))
                {
                    return;
                }
                throw new InvalidOperationException(
                    "Restore target physical boundary is incomplete.",
                    error);
            }
            catch (Exception error) when (error is IOException
                or UnauthorizedAccessException
                or System.Security.SecurityException)
            {
                throw new InvalidOperationException(
                    "Restore target physical boundary cannot be verified.",
                    error);
            }
            if ((attributes & FileAttributes.ReparsePoint) != 0)
            {
                throw new InvalidOperationException(
                    "Restore target traverses a reparse point.");
            }
            bool isDirectory = (attributes & FileAttributes.Directory) != 0;
            bool isLastSegment = index == segments.Length - 1;
            if ((!isLastSegment && !isDirectory) || (isLastSegment && isDirectory))
            {
                throw new InvalidOperationException(
                    "Restore target physical boundary has an unexpected entry type.");
            }
        }
    }

    private static void ValidateManifestTargetBoundaries(
        RestoreJournalInfo journal,
        RestoreSnapshotManifestFile manifest,
        CodexStorageLayout storage)
    {
        if (!PathsEqual(manifest.PreRestoreSnapshot.BackupDir, journal.SnapshotDir))
        {
            throw new InvalidOperationException("Restore storage identity changed.");
        }
        ValidateRestoreHomePhysicalIdentity(manifest.Storage, storage);
        foreach (RestoreJournalTarget target in manifest.Targets)
        {
            switch (target.Kind)
            {
                case "config":
                case "globalState":
                case "rollout":
                    ValidatePhysicalTargetBoundary(
                        target.Kind,
                        target.TargetPath,
                        manifest.Storage,
                        storage);
                    break;
                case "sqlite" when manifest.Storage.TargetStateDbPath is not null
                    && PathsEqual(target.TargetPath, manifest.Storage.TargetStateDbPath):
                    break;
                default:
                    throw new InvalidOperationException(
                        "Restore snapshot contains a target outside the declared storage boundary.");
            }
        }
    }

    private async Task<string> VerifyManifestTargetsAsync(
        RestoreSnapshotManifestFile manifest,
        bool expectedPre,
        string scratchDir,
        CodexStorageLayout storage,
        CancellationToken cancellationToken)
    {
        List<(string Id, string Digest)> values = [];
        foreach (RestoreJournalTarget target in manifest.Targets)
        {
            ValidatePhysicalTargetBoundary(
                target.Kind,
                target.TargetPath,
                manifest.Storage,
                storage);
            RestoreDigest actual = await DigestTargetAsync(
                target,
                scratchDir,
                storage,
                cancellationToken);
            RestoreDigest expected = expectedPre ? target.Pre : target.ExpectedPost;
            if (!SameDigest(actual, expected))
            {
                throw new InvalidOperationException(
                    $"A Restore target {target.Kind} digest does not match durable evidence.");
            }
            values.Add((target.Id, actual.Digest));
        }
        values.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Id, right.Id));
        using MemoryStream json = new();
        using (Utf8JsonWriter writer = new(json, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            writer.WriteStartArray();
            foreach ((string id, string digest) in values)
            {
                writer.WriteStartObject();
                writer.WriteString("digest", digest);
                writer.WriteString("id", id);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Sha256Base64Url(json.ToArray());
    }

    private async Task<RestoreDigest> ExpectedPostDigestAsync(
        RestoreBackupTarget target,
        RestoreDigest pre,
        string scratchDir,
        CodexStorageLayout storage,
        CancellationToken cancellationToken)
    {
        if (target.Kind == "rollout")
        {
            return DigestRolloutEntry(target.SessionEntry
                ?? throw new InvalidOperationException("Restore rollout source entry is missing."));
        }
        if (target.Kind == "sqlite")
        {
            return await DigestSqliteAsync(
                target.SourcePath ?? throw new InvalidOperationException("Restore SQLite source is missing."),
                scratchDir,
                storage,
                cancellationToken);
        }
        if (target.Kind == "globalState" && target.SourceAction == "delete")
        {
            return AbsentDigest();
        }
        if (target.Kind == "globalState" && target.SourceAction == "preserve")
        {
            return pre;
        }
        return await DigestFileAsync(
            target.SourcePath ?? throw new InvalidOperationException("Restore file source is missing."),
            cancellationToken);
    }

    private async Task<RestoreDigest> DigestTargetAsync(
        RestoreJournalTarget target,
        string scratchDir,
        CodexStorageLayout storage,
        CancellationToken cancellationToken) => target.Kind switch
        {
            "rollout" => DigestRolloutEntry(
                await CaptureRolloutEntryAsync(target.TargetPath, cancellationToken)),
            "sqlite" => await DigestSqliteAsync(
                target.TargetPath,
                scratchDir,
                storage,
                cancellationToken),
            _ => await DigestFileAsync(target.TargetPath, cancellationToken)
        };

    private async Task<RestoreDigest> DigestSqliteAsync(
        string sqlitePath,
        string scratchDir,
        CodexStorageLayout storage,
        CancellationToken cancellationToken)
    {
        string fullPath = Path.GetFullPath(sqlitePath);
        if (!File.Exists(fullPath))
        {
            return AbsentDigest();
        }
        Directory.CreateDirectory(scratchDir);
        string scratchPath = Path.Combine(
            scratchDir,
            $".sqlite-digest-{Guid.NewGuid():N}.sqlite");
        try
        {
            SqliteOnlineBackupResult backup = await _sqliteStateService.CreateSqliteOnlineBackupAsync(
                StorageForDatabase(storage, fullPath, "restore-v2-digest"),
                scratchPath);
            if (!backup.DatabasePresent)
            {
                throw new InvalidOperationException(
                    "The State DB disappeared while its Restore digest was captured.");
            }
            byte[] bytes = await File.ReadAllBytesAsync(scratchPath, cancellationToken);
            if (bytes.Length < 100
                || !bytes.AsSpan(0, 16).SequenceEqual("SQLite format 3\0"u8))
            {
                throw new InvalidOperationException("Restore SQLite digest source has an invalid header.");
            }
            // SQLite's online-backup API preserves logical pages but keeps the
            // destination's rollback/WAL header mode and may rewrite the
            // volatile file-change counter pair. None describes logical DB
            // content, and Restore intentionally preserves the live mode.
            bytes.AsSpan(18, 2).Clear();
            bytes.AsSpan(24, 4).Clear();
            bytes.AsSpan(92, 4).Clear();
            bytes.AsSpan(96, 4).Clear();
            return new RestoreDigest(
                true,
                "sha256-sqlite-online-backup",
                Sha256Base64Url(bytes),
                bytes.LongLength);
        }
        finally
        {
            TryDeleteFile(scratchPath);
            TryDeleteFile(scratchPath + "-wal");
            TryDeleteFile(scratchPath + "-shm");
        }
    }

    private static CodexStorageLayout StorageForDatabase(
        CodexStorageLayout storage,
        string databasePath,
        string source)
    {
        string fullPath = Path.GetFullPath(databasePath);
        return storage with
        {
            SqliteHome = Path.GetDirectoryName(fullPath)!,
            StateDbLocation = new StateDbLocation(fullPath, Path.GetFileName(fullPath), source),
            StateDbCandidates = [new StateDbLocation(fullPath, Path.GetFileName(fullPath), source)],
            AllowLegacyRootFallback = false
        };
    }

    private static async Task<RestoreDigest> DigestFileAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        string fullPath = Path.GetFullPath(filePath);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            FileInfo before = new(fullPath);
            if (!before.Exists)
            {
                return AbsentDigest();
            }
            if ((before.Attributes & FileAttributes.Directory) != 0)
            {
                throw new InvalidOperationException("A Restore target is not a regular file.");
            }
            long length = before.Length;
            long lastWriteTicks = before.LastWriteTimeUtc.Ticks;
            byte[] hash;
            await using (FileStream stream = new(
                fullPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                hash = await SHA256.HashDataAsync(stream, cancellationToken);
            }
            FileInfo after = new(fullPath);
            after.Refresh();
            if (after.Exists
                && after.Length == length
                && after.LastWriteTimeUtc.Ticks == lastWriteTicks)
            {
                return new RestoreDigest(
                    true,
                    "sha256-file",
                    Base64Url(hash),
                    after.Length);
            }
        }
        throw new InvalidOperationException(
            "A Restore target changed while its digest was captured.");
    }

    private static async Task<SessionBackupManifestEntry> CaptureRolloutEntryAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        string fullPath = Path.GetFullPath(filePath);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            FileInfo before = new(fullPath);
            if (!before.Exists)
            {
                throw new FileNotFoundException("Restore rollout target is missing.", fullPath);
            }
            long length = before.Length;
            long lastWriteTicks = before.LastWriteTimeUtc.Ticks;
            (string firstLine, string separator) = await ReadFirstLineAsync(fullPath, cancellationToken);
            using (JsonDocument first = JsonDocument.Parse(firstLine))
            {
                if (!first.RootElement.TryGetProperty("type", out JsonElement type)
                    || type.GetString() != "session_meta")
                {
                    throw new InvalidOperationException(
                        "Rollout does not start with a valid session_meta record.");
                }
            }
            List<TurnContextModelBackup> models = [];
            using (StreamReader reader = new(
                new FileStream(
                    fullPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete,
                    64 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan),
                new UTF8Encoding(false, true),
                detectEncodingFromByteOrderMarks: false))
            {
                _ = await reader.ReadLineAsync(cancellationToken);
                int lineIndex = 0;
                while (await reader.ReadLineAsync(cancellationToken) is { } line)
                {
                    lineIndex++;
                    if (!line.Contains("\"turn_context\"", StringComparison.Ordinal))
                    {
                        continue;
                    }
                    if (!TurnContextTypeRegex.IsMatch(line))
                    {
                        continue;
                    }
                    string[] values = ModelFieldRegex.Matches(line)
                        .Select(match => JsonSerializer.Deserialize<string>(match.Groups["value"].Value))
                        .Where(static value => value is not null)
                        .Cast<string>()
                        .ToArray();
                    if (values.Length > 0)
                    {
                        models.Add(new TurnContextModelBackup
                        {
                            LineIndex = lineIndex,
                            OriginalModel = values[0],
                            OriginalModels = values
                        });
                    }
                }
            }
            FileInfo after = new(fullPath);
            after.Refresh();
            if (after.Exists
                && after.Length == length
                && after.LastWriteTimeUtc.Ticks == lastWriteTicks)
            {
                DateTimeOffset original = new(new DateTime(lastWriteTicks, DateTimeKind.Utc));
                return new SessionBackupManifestEntry
                {
                    Path = fullPath,
                    OriginalFirstLine = firstLine,
                    OriginalSeparator = separator,
                    OriginalLastWriteTimeUtc = original.ToString(
                        "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                        System.Globalization.CultureInfo.InvariantCulture),
                    OriginalMtimeMs = original.ToUnixTimeMilliseconds(),
                    OriginalLastWriteTimeUtcTicks = lastWriteTicks,
                    ModelOnlyChange = false,
                    OriginalTurnContextModels = models
                };
            }
        }
        throw new InvalidOperationException(
            "Rollout changed while its recovery metadata was captured.");
    }

    private static async Task<(string FirstLine, string Separator)> ReadFirstLineAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        await using FileStream stream = new(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using MemoryStream bytes = new();
        byte[] one = new byte[1];
        while (await stream.ReadAsync(one, cancellationToken) == 1)
        {
            if (one[0] == (byte)'\n')
            {
                byte[] value = bytes.ToArray();
                bool crlf = value.Length > 0 && value[^1] == (byte)'\r';
                int length = crlf ? value.Length - 1 : value.Length;
                return (new UTF8Encoding(false, true).GetString(value, 0, length), crlf ? "\r\n" : "\n");
            }
            bytes.WriteByte(one[0]);
            if (bytes.Length > 16 * 1024 * 1024)
            {
                throw new InvalidOperationException("Rollout session_meta record is unreasonably large.");
            }
        }
        return (new UTF8Encoding(false, true).GetString(bytes.ToArray()), "\n");
    }

    private static RestoreDigest DigestRolloutEntry(SessionBackupManifestEntry entry)
    {
        using MemoryStream json = new();
        using (Utf8JsonWriter writer = new(json, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            writer.WriteStartObject();
            writer.WriteString("originalFirstLine", entry.OriginalFirstLine);
            writer.WriteString("originalSeparator", entry.OriginalSeparator ?? "\n");
            writer.WriteStartArray("originalTurnContextModels");
            foreach (TurnContextModelBackup model in entry.OriginalTurnContextModels)
            {
                writer.WriteStartObject();
                writer.WriteNumber("lineIndex", model.LineIndex);
                writer.WriteString("originalModel", model.OriginalModel);
                writer.WriteStartArray("originalModels");
                foreach (string value in model.OriginalModels)
                {
                    writer.WriteStringValue(value);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        return new RestoreDigest(
            true,
            "sha256-rollout-metadata",
            Sha256Base64Url(json.ToArray()));
    }

    private static async Task CollectIdentityFilesAsync(
        string root,
        string current,
        List<(string Path, string Sha256)> files,
        CancellationToken cancellationToken)
    {
        foreach (FileSystemInfo entry in new DirectoryInfo(current)
            .EnumerateFileSystemInfos()
            .OrderBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if ((entry.Attributes & FileAttributes.ReparsePoint) != 0)
            {
                throw new InvalidOperationException(
                    "A managed Restore source contains an unsupported linked entry.");
            }
            if ((entry.Attributes & FileAttributes.Directory) != 0)
            {
                await CollectIdentityFilesAsync(root, entry.FullName, files, cancellationToken);
            }
            else
            {
                RestoreDigest digest = await DigestFileAsync(entry.FullName, cancellationToken);
                files.Add((
                    Path.GetRelativePath(root, entry.FullName).Replace(Path.DirectorySeparatorChar, '/'),
                    digest.Digest));
            }
        }
    }

    private static string SnapshotRelativePath(RestoreBackupTarget target) => target.Kind switch
    {
        "config" => "config.toml",
        "globalState" => Path.GetFileName(target.TargetPath),
        "sqlite" => Path.Combine("db", "sqlite-home", AppConstants.DbFileBasename),
        _ => throw new InvalidOperationException(
            $"Restore target {target.Kind} does not use a file snapshot path.")
    };

    private static string SafeSnapshotPath(string snapshotDir, string? relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath) || Path.IsPathRooted(relativePath))
        {
            throw new InvalidOperationException("Restore snapshot path is missing or rooted.");
        }
        string root = Path.GetFullPath(snapshotDir);
        string fullPath = Path.GetFullPath(Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar)));
        string relative = Path.GetRelativePath(root, fullPath);
        if (relative == ".."
            || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || Path.IsPathRooted(relative))
        {
            throw new InvalidOperationException("Restore snapshot path escapes its managed directory.");
        }
        return fullPath;
    }

    private static RestoreResult BuildResult(
        RestoreBackupPlan plan,
        string operationId,
        string snapshotId,
        string journalState,
        IReadOnlyList<string> resolvedOperationIds,
        bool commitAcknowledgementRecovered) => new()
    {
        CodexHome = plan.Storage.CodexHome,
        BackupDir = plan.BackupDirectory,
        TargetProvider = plan.Metadata.TargetProvider,
        CreatedAt = plan.Metadata.CreatedAt,
        ChangedSessionFiles = plan.Metadata.ChangedSessionFiles,
        RestoreVersion = 2,
        RestoreOperationId = operationId,
        PreRestoreSnapshotId = snapshotId,
        RestoreJournalState = journalState,
        CommitAcknowledgementRecovered = commitAcknowledgementRecovered,
        ResolvedOperationIds = resolvedOperationIds
    };

    private static RestoreDigest AbsentDigest() => new(
        false,
        "absent",
        Sha256Base64Url(Encoding.UTF8.GetBytes("absent")));

    private static bool SameDigest(RestoreDigest left, RestoreDigest right) =>
        left.Present == right.Present
        && left.DigestKind == right.DigestKind
        && left.Digest == right.Digest;

    private static string TargetId(string kind, string targetPath) =>
        Sha256Base64Url(Encoding.UTF8.GetBytes(
            kind + "\0" + ComparablePath(targetPath)));

    private static string TargetKey(string kind, string targetPath) =>
        kind + "\0" + ComparablePath(targetPath);

    private static string ComparablePath(string value)
    {
        string fullPath = Path.GetFullPath(value);
        return OperatingSystem.IsWindows() ? fullPath.ToLowerInvariant() : fullPath;
    }

    private static string Sha256Base64Url(byte[] bytes) =>
        Base64Url(SHA256.HashData(bytes));

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static bool PathsEqual(string left, string right) => string.Equals(
        Path.GetFullPath(left),
        Path.GetFullPath(right),
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);

    private static string ReadSourceTargetProvider(string backupDir)
    {
        try
        {
            BackupMetadataFile? metadata = JsonSerializer.Deserialize<BackupMetadataFile>(
                File.ReadAllText(Path.Combine(backupDir, "metadata.json")),
                JsonOptions);
            return metadata?.TargetProvider ?? AppConstants.DefaultProvider;
        }
        catch
        {
            return AppConstants.DefaultProvider;
        }
    }

    private static string ErrorCode(Exception error) => error switch
    {
        RecoveryRequiredException => "RECOVERY_REQUIRED",
        OperationCanceledException => "CANCELLED",
        SyncTransactionException transaction => transaction.Code,
        _ => "RESTORE_FAILED"
    };

    private static RecoveryRequiredException RecoveryRequired(
        string message,
        RestorePreSnapshot snapshot,
        RestoreBackupIdentity sourceBackup,
        Exception cause)
    {
        RecoveryRequiredException error = new(
            message + " " + cause.Message,
            [snapshot.BackupDirectory, sourceBackup.BackupDir]);
        return error;
    }

    private static async Task<RestoreJournalInfo> SafeReadAsync(
        string filePath,
        RestoreJournalInfo fallback)
    {
        try
        {
            return await RestoreJournalService.ReadInfoAsync(filePath, CancellationToken.None);
        }
        catch
        {
            return fallback;
        }
    }

    private static async Task TryMarkRecoveryRequiredAsync(
        RestoreJournalInfo journal,
        string reasonCode)
    {
        try
        {
            if (!journal.InvalidTail
                && journal.State != "recovery-required"
                && !journal.Terminal)
            {
                await RestoreJournal.Reopen(journal).RecoveryRequiredAsync(
                    reasonCode,
                    CancellationToken.None);
            }
        }
        catch
        {
            // Existing journal evidence remains the authoritative blocker.
        }
    }

    private async Task InvokeFaultAsync(string point, string? targetPath, int count)
    {
        if (FaultInjector is not null)
        {
            await FaultInjector(point, targetPath, count);
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // Cleanup must not hide the primary error.
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Scratch cleanup must not hide the primary result.
        }
    }
}
