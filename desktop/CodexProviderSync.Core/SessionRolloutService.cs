using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace CodexProviderSync.Core;

public sealed class SessionRolloutService
{
    private const string StatusOnlyProvider = "__status_only__";
    private const int ScanBufferSize = 1024 * 1024;

    public async Task<SessionChangeCollection> CollectSessionChangesAsync(
        string codexHome,
        string targetProvider,
        bool skipLockedReads = false,
        string? targetModel = null)
    {
        List<SessionChange> changes = [];
        List<string> lockedPaths = [];
        List<string> unreadablePaths = [];
        Dictionary<string, int> sessionCounts = new(StringComparer.Ordinal);
        Dictionary<string, int> archivedCounts = new(StringComparer.Ordinal);
        Dictionary<string, int> encryptedSessionCounts = new(StringComparer.Ordinal);
        Dictionary<string, int> encryptedArchivedCounts = new(StringComparer.Ordinal);
        HashSet<string> userEventThreadIds = new(StringComparer.Ordinal);
        Dictionary<string, string> threadCwdsById = new(StringComparer.Ordinal);

        foreach (string dirName in AppConstants.SessionDirectories)
        {
            string rootDir = Path.Combine(codexHome, dirName);
            if (!Directory.Exists(rootDir))
            {
                continue;
            }

            foreach (string rolloutPath in Directory.EnumerateFiles(rootDir, "rollout-*.jsonl", SearchOption.AllDirectories))
            {
                FirstLineRecord record;
                try
                {
                    record = await ReadFirstLineRecordAsync(rolloutPath);
                }
                catch (Exception error) when (skipLockedReads && IsRolloutFileBusyError(error))
                {
                    lockedPaths.Add(rolloutPath);
                    continue;
                }
                catch (Exception error) when (skipLockedReads && IsRolloutFileUnreadableError(error))
                {
                    unreadablePaths.Add(rolloutPath);
                    continue;
                }

                if (!TryParseSessionMetaRecord(record.FirstLine, out JsonObject? root, out JsonObject? payload))
                {
                    continue;
                }

                string currentProvider = payload!["model_provider"]?.GetValue<string>() ?? "(missing)";
                Dictionary<string, int> bucket = dirName == "archived_sessions" ? archivedCounts : sessionCounts;
                bucket[currentProvider] = bucket.TryGetValue(currentProvider, out int count) ? count + 1 : 1;
                if (payload["id"]?.GetValue<string>() is string metadataThreadId
                    && !string.IsNullOrWhiteSpace(metadataThreadId)
                    && payload["cwd"]?.GetValue<string>() is string metadataCwd
                    && !string.IsNullOrWhiteSpace(metadataCwd))
                {
                    threadCwdsById[metadataThreadId] = ToDesktopWorkspacePath(metadataCwd);
                }
                bool hasEncryptedContent;
                try
                {
                    hasEncryptedContent = await FileHasEncryptedContentAsync(rolloutPath, record.FirstLine, record.Offset);
                    if (payload["id"]?.GetValue<string>() is string threadId
                        && await FileHasUserEventAsync(rolloutPath, record.FirstLine, record.Offset))
                    {
                        userEventThreadIds.Add(threadId);
                    }
                }
                catch (Exception error) when (skipLockedReads && IsRolloutFileBusyError(error))
                {
                    lockedPaths.Add(rolloutPath);
                    continue;
                }
                catch (Exception error) when (skipLockedReads && IsRolloutFileUnreadableError(error))
                {
                    unreadablePaths.Add(rolloutPath);
                    continue;
                }

                if (hasEncryptedContent)
                {
                    Dictionary<string, int> encryptedBucket = dirName == "archived_sessions" ? encryptedArchivedCounts : encryptedSessionCounts;
                    encryptedBucket[currentProvider] = encryptedBucket.TryGetValue(currentProvider, out int encryptedCount) ? encryptedCount + 1 : 1;
                }

                bool providerChanged = !string.Equals(targetProvider, StatusOnlyProvider, StringComparison.Ordinal)
                    && !string.Equals(currentProvider, targetProvider, StringComparison.Ordinal);
                IReadOnlyList<string> currentModels = [];
                bool modelChanged = false;
                if (!string.IsNullOrEmpty(targetModel))
                {
                    try
                    {
                        currentModels = await ReadTurnContextModelsAsync(rolloutPath, record);
                        modelChanged = currentModels.Any(model => !string.Equals(model, targetModel, StringComparison.Ordinal));
                    }
                    catch (Exception error) when (skipLockedReads && IsRolloutFileBusyError(error))
                    {
                        lockedPaths.Add(rolloutPath);
                        continue;
                    }
                    catch (Exception error) when (skipLockedReads && IsRolloutFileUnreadableError(error))
                    {
                        unreadablePaths.Add(rolloutPath);
                        continue;
                    }
                }

                if (providerChanged || modelChanged)
                {
                    FileSnapshot snapshot = GetFileSnapshot(rolloutPath);
                    if (providerChanged)
                    {
                        payload["model_provider"] = targetProvider;
                    }
                    changes.Add(new SessionChange
                    {
                        Path = rolloutPath,
                        ThreadId = payload["id"]?.GetValue<string>(),
                        Directory = dirName,
                        OriginalFirstLine = record.FirstLine,
                        OriginalSeparator = record.Separator,
                        OriginalOffset = record.Offset,
                        OriginalFileLength = snapshot.Length,
                        OriginalLastWriteTimeUtcTicks = snapshot.LastWriteTimeUtcTicks,
                        OriginalProvider = currentProvider,
                        UpdatedFirstLine = providerChanged ? root!.ToJsonString() : record.FirstLine,
                        ModelOnlyChange = !providerChanged && modelChanged
                    });
                }
            }
        }

        return new SessionChangeCollection
        {
            Changes = changes,
            LockedPaths = lockedPaths.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList(),
            UnreadablePaths = unreadablePaths.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList(),
            ProviderCounts = new ProviderCounts
            {
                Sessions = sessionCounts,
                ArchivedSessions = archivedCounts
            },
            EncryptedContentCounts = new ProviderCounts
            {
                Sessions = encryptedSessionCounts,
                ArchivedSessions = encryptedArchivedCounts
            },
            UserEventThreadIds = userEventThreadIds,
            ThreadCwdsById = threadCwdsById
        };
    }

    public async Task<SessionApplyResult> ApplySessionChangesAsync(
        IEnumerable<SessionChange> changes,
        string? targetModel = null)
    {
        int appliedCount = 0;
        List<string> appliedPaths = [];
        List<string> skippedPaths = [];

        foreach (SessionChange change in changes)
        {
            bool providerApplied = change.ModelOnlyChange || await TryRewriteCollectedSessionChangeAsync(change);
            if (!providerApplied)
            {
                skippedPaths.Add(change.Path);
                continue;
            }

            ModelRewriteResult modelResult = ModelRewriteResult.Empty;
            try
            {
                if (!string.IsNullOrEmpty(targetModel))
                {
                    modelResult = await TryRewriteRolloutModelFieldAsync(change, targetModel);
                    change.OriginalTurnContextModels = modelResult.OriginalModels;
                }
            }
            catch
            {
                if (!change.ModelOnlyChange)
                {
                    await RewriteFirstLineAsync(
                        change.Path,
                        change.OriginalFirstLine,
                        change.OriginalSeparator);
                }
                TryRestoreLastWriteTimeUtc(change.Path, change.OriginalLastWriteTimeUtcTicks);
                throw;
            }
            if (change.ModelOnlyChange && modelResult.ReplacedLines == 0)
            {
                skippedPaths.Add(change.Path);
                continue;
            }

            TryRestoreLastWriteTimeUtc(change.Path, change.OriginalLastWriteTimeUtcTicks);
            appliedCount += 1;
            appliedPaths.Add(change.Path);
        }

        appliedPaths.Sort(StringComparer.Ordinal);
        skippedPaths.Sort(StringComparer.Ordinal);
        return new SessionApplyResult
        {
            AppliedCount = appliedCount,
            AppliedPaths = appliedPaths,
            SkippedPaths = skippedPaths
        };
    }

    public async Task AssertSessionFilesWritableAsync(IEnumerable<string> filePaths)
    {
        List<string> lockedPaths = await FindLockedFilesAsync(filePaths);
        if (lockedPaths.Count == 0)
        {
            return;
        }

        string preview = string.Join(", ", lockedPaths.Take(5));
        int extraCount = lockedPaths.Count - Math.Min(lockedPaths.Count, 5);
        string suffix = extraCount > 0 ? $" (+{extraCount} more)" : string.Empty;
        throw new InvalidOperationException(
            $"Unable to rewrite rollout files because {lockedPaths.Count} file(s) are currently in use. Close Codex and the Codex app, then retry. Locked file(s): {preview}{suffix}");
    }

    public async Task<(IReadOnlyList<SessionChange> WritableChanges, IReadOnlyList<SessionChange> LockedChanges)> SplitLockedSessionChangesAsync(
        IEnumerable<SessionChange> changes)
    {
        List<SessionChange> changeList = changes.ToList();
        List<string> lockedPaths = await FindLockedFilesAsync(changeList.Select(static change => change.Path));
        if (lockedPaths.Count == 0)
        {
            return (changeList, []);
        }

        HashSet<string> lockedSet = new(lockedPaths, StringComparer.Ordinal);
        List<SessionChange> writable = [];
        List<SessionChange> locked = [];
        foreach (SessionChange change in changeList)
        {
            if (lockedSet.Contains(change.Path))
            {
                locked.Add(change);
            }
            else
            {
                writable.Add(change);
            }
        }

        return (writable, locked);
    }

    internal async Task RestoreSessionChangesAsync(IEnumerable<SessionBackupManifestEntry> manifestEntries)
    {
        foreach (SessionBackupManifestEntry entry in manifestEntries)
        {
            if (!entry.ModelOnlyChange)
            {
                await RewriteFirstLineAsync(entry.Path, entry.OriginalFirstLine, entry.OriginalSeparator);
            }
            if (entry.OriginalTurnContextModels.Count > 0)
            {
                await RestoreTurnContextModelsAsync(
                    entry.Path,
                    entry.OriginalTurnContextModels,
                    entry.OriginalSeparator);
            }
            TryRestoreLastWriteTimeUtc(entry.Path, entry.OriginalLastWriteTimeUtcTicks);
        }
    }

    internal Task RestoreSessionChangesAsync(IEnumerable<SessionChange> changes)
    {
        return RestoreSessionChangesAsync(
            changes.Select(static change => new SessionBackupManifestEntry
            {
                Path = change.Path,
                OriginalFirstLine = change.OriginalFirstLine,
                OriginalSeparator = change.OriginalSeparator,
                OriginalLastWriteTimeUtcTicks = change.OriginalLastWriteTimeUtcTicks,
                ModelOnlyChange = change.ModelOnlyChange,
                OriginalTurnContextModels = [.. change.OriginalTurnContextModels]
            }));
    }

    private static bool TryParseSessionMetaRecord(
        string firstLine,
        out JsonObject? root,
        out JsonObject? payload)
    {
        root = null;
        payload = null;

        if (string.IsNullOrWhiteSpace(firstLine))
        {
            return false;
        }

        try
        {
            root = JsonNode.Parse(firstLine) as JsonObject;
            if (root?["type"]?.GetValue<string>() != "session_meta")
            {
                return false;
            }

            payload = root["payload"] as JsonObject;
            return payload is not null;
        }
        catch
        {
            return false;
        }
    }

    private async Task<FirstLineRecord> ReadFirstLineRecordAsync(string filePath)
    {
        try
        {
            await using FileStream stream = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            return await ReadFirstLineRecordAsync(stream);
        }
        catch (Exception error)
        {
            throw WrapRolloutFileBusyError(error, filePath, "read");
        }
    }

    private async Task<bool> TryRewriteCollectedSessionChangeAsync(SessionChange change)
    {
        try
        {
            await using FileStream sourceStream = OpenExclusiveRewriteStream(change.Path);
            if (sourceStream.Length != change.OriginalFileLength)
            {
                return false;
            }

            FirstLineRecord current = await ReadFirstLineRecordAsync(sourceStream);
            if (!string.Equals(current.FirstLine, change.OriginalFirstLine, StringComparison.Ordinal)
                || current.Offset != change.OriginalOffset)
            {
                return false;
            }

            await RewriteFirstLineAsync(
                sourceStream,
                change.Path,
                change.UpdatedFirstLine!,
                change.OriginalSeparator,
                change.OriginalOffset,
                headerOnly: change.OriginalOffset >= change.OriginalFileLength);
            return true;
        }
        catch (Exception error) when (IsRolloutFileBusyError(error))
        {
            return false;
        }
    }

    private async Task RewriteFirstLineAsync(string filePath, string nextFirstLine, string separator)
    {
        try
        {
            await using FileStream sourceStream = OpenExclusiveRewriteStream(filePath);
            FirstLineRecord current = await ReadFirstLineRecordAsync(sourceStream);
            bool headerOnly = string.IsNullOrEmpty(current.Separator)
                && current.Offset == Encoding.UTF8.GetByteCount(current.FirstLine);
            await RewriteFirstLineAsync(sourceStream, filePath, nextFirstLine, separator, current.Offset, headerOnly);
        }
        catch (Exception error)
        {
            throw WrapRolloutFileBusyError(error, filePath, "rewrite");
        }
    }

    private static FileStream OpenExclusiveRewriteStream(string filePath)
    {
        try
        {
            return new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
        catch (Exception error)
        {
            throw WrapRolloutFileBusyError(error, filePath, "rewrite");
        }
    }

    private static async Task<FirstLineRecord> ReadFirstLineRecordAsync(FileStream stream)
    {
        stream.Seek(0, SeekOrigin.Begin);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            using MemoryStream collected = new();
            while (true)
            {
                int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length));
                if (bytesRead == 0)
                {
                    break;
                }

                await collected.WriteAsync(buffer.AsMemory(0, bytesRead));
                ReadOnlySpan<byte> current = collected.GetBuffer().AsSpan(0, (int)collected.Length);
                int newlineIndex = current.IndexOf((byte)'\n');
                if (newlineIndex >= 0)
                {
                    bool crlf = newlineIndex > 0 && current[newlineIndex - 1] == '\r';
                    int lineLength = crlf ? newlineIndex - 1 : newlineIndex;
                    string firstLine = Encoding.UTF8.GetString(current[..lineLength]);
                    return new FirstLineRecord(firstLine, crlf ? "\r\n" : "\n", newlineIndex + 1);
                }
            }

            string text = Encoding.UTF8.GetString(collected.GetBuffer(), 0, (int)collected.Length);
            return new FirstLineRecord(text, string.Empty, (int)collected.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    // Scan the start of a rollout file looking for the first
    // `turn_context` event and return its `payload.model` field.
    // This is the field that the Codex GUI bottom-right of an old
    // conversation reads, so we have to capture it here and rewrite
    // it (along with `payload.collaboration_mode.settings.model`)
    // on every sync, in addition to the per-thread SQLite `model`
    // column. We stream line-by-line because individual
    // `turn_context` lines can easily exceed 64 KB once Codex
    // embeds a `developer_instructions` blob into the payload — the
    // previous 64 KB scanner silently missed those, which made the
    // rollout model rewrite a no-op for sessions whose first turn
    // was a long planning step. We stop as soon as we find a
    // matching line.
    // `turn_context` after the leading `session_meta` line is
    // enough to know what model the rest of the file uses.
    private static readonly Regex TurnContextTypeRegex = new(
        "\"type\"\\s*:\\s*\"turn_context\"",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex TurnContextModelFieldRegex = new(
        "\"model\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static async Task<IReadOnlyList<string>> ReadTurnContextModelsAsync(
        string rolloutPath,
        FirstLineRecord record)
    {
        List<string> models = [];
        try
        {
            await using FileStream stream = new(
                rolloutPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            stream.Seek(record.Offset, SeekOrigin.Begin);
            using StreamReader reader = new(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);
            string? line;
            while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) is not null)
            {
                if (!TurnContextTypeRegex.IsMatch(line))
                {
                    continue;
                }

                foreach (Match match in TurnContextModelFieldRegex.Matches(line))
                {
                    try
                    {
                        string? model = JsonSerializer.Deserialize<string>($"\"{match.Groups[1].Value}\"");
                        if (!string.IsNullOrEmpty(model))
                        {
                            models.Add(model);
                        }
                    }
                    catch (JsonException)
                    {
                        // Leave malformed model literals untouched.
                    }
                }
            }
            return models;
        }
        catch (Exception error) when (IsRolloutFileBusyError(error))
        {
            throw WrapRolloutFileBusyError(error, rolloutPath, "read");
        }
    }

    // Rewrite the per-turn `model` field in every `turn_context`
    // event of the rollout. The Codex GUI bottom-right of an old
    // conversation reads that field, so we have to keep it aligned
    // with the active root-level model. We do a line-by-line
    // regex rewrite instead of round-tripping the JSON tree to
    // avoid mangling the multi-megabyte `developer_instructions`
    // blob Codex writes into every `turn_context`.
    private async Task<ModelRewriteResult> TryRewriteRolloutModelFieldAsync(
        SessionChange change,
        string targetModel)
    {
        if (string.IsNullOrEmpty(targetModel))
        {
            return ModelRewriteResult.Empty;
        }

        string tempPath = $"{change.Path}.provider-sync-model.{Environment.ProcessId}.{DateTime.UtcNow.Ticks}.{Guid.NewGuid():N}.tmp";
        try
        {
            await using FileStream sourceStream = OpenExclusiveRewriteStream(change.Path);
            if (change.ModelOnlyChange
                && (sourceStream.Length != change.OriginalFileLength
                    || File.GetLastWriteTimeUtc(change.Path).Ticks != change.OriginalLastWriteTimeUtcTicks))
            {
                return ModelRewriteResult.Empty;
            }

            bool hasTrailingNewline = await EndsWithNewlineAsync(sourceStream);
            sourceStream.Seek(0, SeekOrigin.Begin);
            string separator = change.OriginalSeparator == "\r\n" ? "\r\n" : "\n";
            List<TurnContextModelBackup> originalModels = [];
            int replacements = 0;

            using (StreamReader reader = new(
                sourceStream,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 64 * 1024,
                leaveOpen: true))
            {
                await using FileStream writeStream = new(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None);
                await using StreamWriter writer = new(writeStream, new UTF8Encoding(false), 64 * 1024);
                bool firstLine = true;
                int lineIndex = 0;
                string? line;
                while ((line = await reader.ReadLineAsync()) is not null)
                {
                    ModelLineRewrite lineResult = firstLine
                        ? new ModelLineRewrite(line, false, [])
                        : RewriteTurnContextModelInLine(line, targetModel);
                    if (lineResult.Replaced)
                    {
                        replacements += 1;
                        originalModels.Add(new TurnContextModelBackup
                        {
                            LineIndex = lineIndex,
                            OriginalModel = lineResult.OriginalModels[0],
                            OriginalModels = lineResult.OriginalModels
                        });
                    }
                    if (!firstLine)
                    {
                        await writer.WriteAsync(separator);
                    }
                    firstLine = false;
                    await writer.WriteAsync(lineResult.Line);
                    lineIndex += 1;
                }
                if (hasTrailingNewline && !firstLine)
                {
                    await writer.WriteAsync(separator);
                }
            }

            if (replacements == 0)
            {
                File.Delete(tempPath);
                return ModelRewriteResult.Empty;
            }

            await OverwriteOpenFileFromTempAsync(sourceStream, tempPath);

            File.Delete(tempPath);
            return new ModelRewriteResult(replacements, originalModels);
        }
        catch (Exception error)
        {
            try
            {
                File.Delete(tempPath);
            }
            catch
            {
                // Ignore cleanup failures and surface the original error.
            }
            throw WrapRolloutFileBusyError(error, change.Path, "rewrite model field");
        }
    }

    private static ModelLineRewrite RewriteTurnContextModelInLine(string line, string newModel)
    {
        if (!TurnContextTypeRegex.IsMatch(line))
        {
            return new ModelLineRewrite(line, false, []);
        }

        MatchCollection matches = TurnContextModelFieldRegex.Matches(line);
        if (matches.Count == 0)
        {
            return new ModelLineRewrite(line, false, []);
        }

        List<string> originals = [];
        try
        {
            foreach (Match match in matches)
            {
                string? model = JsonSerializer.Deserialize<string>($"\"{match.Groups[1].Value}\"");
                if (model is null)
                {
                    return new ModelLineRewrite(line, false, []);
                }
                originals.Add(model);
            }
        }
        catch (JsonException)
        {
            return new ModelLineRewrite(line, false, []);
        }

        if (originals.All(model => string.Equals(model, newModel, StringComparison.Ordinal)))
        {
            return new ModelLineRewrite(line, false, originals);
        }

        string encodedModel = JsonSerializer.Serialize(newModel);
        string rewritten = TurnContextModelFieldRegex.Replace(line, $"\"model\":{encodedModel}");
        return new ModelLineRewrite(rewritten, true, originals);
    }

    private static async Task RestoreTurnContextModelsAsync(
        string filePath,
        IReadOnlyList<TurnContextModelBackup> backups,
        string originalSeparator)
    {
        Dictionary<int, TurnContextModelBackup> backupsByLine = backups
            .GroupBy(static backup => backup.LineIndex)
            .ToDictionary(static group => group.Key, static group => group.Last());
        if (backupsByLine.Count == 0)
        {
            return;
        }

        string tempPath = $"{filePath}.provider-sync-model-restore.{Environment.ProcessId}.{DateTime.UtcNow.Ticks}.{Guid.NewGuid():N}.tmp";
        try
        {
            await using FileStream sourceStream = OpenExclusiveRewriteStream(filePath);
            bool hasTrailingNewline = await EndsWithNewlineAsync(sourceStream);
            sourceStream.Seek(0, SeekOrigin.Begin);
            string separator = originalSeparator == "\r\n" ? "\r\n" : "\n";
            int replacements = 0;

            using (StreamReader reader = new(
                sourceStream,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 64 * 1024,
                leaveOpen: true))
            {
                await using FileStream writeStream = new(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None);
                await using StreamWriter writer = new(writeStream, new UTF8Encoding(false), 64 * 1024);
                bool firstLine = true;
                int lineIndex = 0;
                string? line;
                while ((line = await reader.ReadLineAsync()) is not null)
                {
                    string next = line;
                    if (!firstLine && backupsByLine.TryGetValue(lineIndex, out TurnContextModelBackup? backup))
                    {
                        next = RestoreTurnContextModelInLine(line, backup);
                        if (!string.Equals(next, line, StringComparison.Ordinal))
                        {
                            replacements += 1;
                        }
                    }
                    if (!firstLine)
                    {
                        await writer.WriteAsync(separator);
                    }
                    firstLine = false;
                    await writer.WriteAsync(next);
                    lineIndex += 1;
                }
                if (hasTrailingNewline && !firstLine)
                {
                    await writer.WriteAsync(separator);
                }
            }

            if (replacements == 0)
            {
                File.Delete(tempPath);
                return;
            }

            await OverwriteOpenFileFromTempAsync(sourceStream, tempPath);
            File.Delete(tempPath);
        }
        catch (Exception error)
        {
            try
            {
                File.Delete(tempPath);
            }
            catch
            {
                // Ignore cleanup failures and surface the original error.
            }
            throw WrapRolloutFileBusyError(error, filePath, "restore model field");
        }
    }

    private static string RestoreTurnContextModelInLine(
        string line,
        TurnContextModelBackup backup)
    {
        if (!TurnContextTypeRegex.IsMatch(line))
        {
            return line;
        }

        MatchCollection matches = TurnContextModelFieldRegex.Matches(line);
        if (matches.Count == 0)
        {
            return line;
        }

        IReadOnlyList<string> originals = backup.OriginalModels.Count == matches.Count
            ? backup.OriginalModels
            : Enumerable.Repeat(backup.OriginalModel, matches.Count).ToList();
        int index = 0;
        return TurnContextModelFieldRegex.Replace(
            line,
            _ => $"\"model\":{JsonSerializer.Serialize(originals[index++])}");
    }

    private static async Task<bool> EndsWithNewlineAsync(FileStream stream)
    {
        if (stream.Length == 0)
        {
            return false;
        }

        stream.Seek(-1, SeekOrigin.End);
        byte[] tail = new byte[1];
        int bytesRead = await stream.ReadAsync(tail);
        return bytesRead == 1 && tail[0] == (byte)'\n';
    }

    private static async Task OverwriteOpenFileFromTempAsync(
        FileStream destination,
        string tempPath)
    {
        string rollbackPath = $"{tempPath}.rollback";
        try
        {
            destination.Seek(0, SeekOrigin.Begin);
            await using (FileStream rollbackWriter = new(
                rollbackPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                await destination.CopyToAsync(rollbackWriter);
                await rollbackWriter.FlushAsync();
            }

            try
            {
                await using FileStream tempReader = new(
                    tempPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                destination.SetLength(0);
                destination.Seek(0, SeekOrigin.Begin);
                await tempReader.CopyToAsync(destination);
                await destination.FlushAsync();
            }
            catch
            {
                await using FileStream rollbackReader = new(
                    rollbackPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                destination.SetLength(0);
                destination.Seek(0, SeekOrigin.Begin);
                await rollbackReader.CopyToAsync(destination);
                await destination.FlushAsync();
                throw;
            }
        }
        finally
        {
            try
            {
                File.Delete(rollbackPath);
            }
            catch
            {
                // The original exception, if any, is more useful than cleanup failure.
            }
        }
    }

    private static async Task RewriteFirstLineAsync(
        FileStream sourceStream,
        string filePath,
        string nextFirstLine,
        string separator,
        int sourceOffset,
        bool headerOnly)
    {
        string tempPath = $"{filePath}.provider-sync.{Environment.ProcessId}.{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.tmp";

        try
        {
            await using (FileStream writer = new(
                tempPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                byte[] firstLineBytes = Encoding.UTF8.GetBytes(nextFirstLine);
                await writer.WriteAsync(firstLineBytes);
                if (!string.IsNullOrEmpty(separator))
                {
                    byte[] separatorBytes = Encoding.UTF8.GetBytes(separator);
                    await writer.WriteAsync(separatorBytes);
                }

                if (!headerOnly)
                {
                    sourceStream.Seek(sourceOffset, SeekOrigin.Begin);
                    await sourceStream.CopyToAsync(writer);
                }
            }

            await using (FileStream tempReader = new(
                tempPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                sourceStream.SetLength(0);
                sourceStream.Seek(0, SeekOrigin.Begin);
                await tempReader.CopyToAsync(sourceStream);
                await sourceStream.FlushAsync();
            }

            File.Delete(tempPath);
        }
        catch
        {
            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
            catch
            {
                // Ignore cleanup failures and surface the original error.
            }

            throw;
        }
    }

    private static FileSnapshot GetFileSnapshot(string filePath)
    {
        FileInfo fileInfo = new(filePath);
        return new FileSnapshot(fileInfo.Length, fileInfo.LastWriteTimeUtc.Ticks);
    }

    private static async Task<bool> FileContainsTextAsync(string filePath, string text, int startOffset)
    {
        byte[] needle = Encoding.UTF8.GetBytes(text);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ScanBufferSize);
        byte[] tail = [];

        try
        {
            await using FileStream stream = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                ScanBufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            if (startOffset > 0)
            {
                stream.Seek(startOffset, SeekOrigin.Begin);
            }

            while (true)
            {
                int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, ScanBufferSize));
                if (bytesRead == 0)
                {
                    return false;
                }

                byte[] haystack = buffer;
                int haystackLength = bytesRead;
                if (tail.Length > 0)
                {
                    haystackLength = tail.Length + bytesRead;
                    haystack = ArrayPool<byte>.Shared.Rent(haystackLength);
                    Buffer.BlockCopy(tail, 0, haystack, 0, tail.Length);
                    Buffer.BlockCopy(buffer, 0, haystack, tail.Length, bytesRead);
                }

                try
                {
                    if (ContainsNeedle(haystack, haystackLength, needle))
                    {
                        return true;
                    }

                    int keepBytes = Math.Min(Math.Max(0, needle.Length - 1), haystackLength);
                    if (keepBytes == 0)
                    {
                        tail = [];
                    }
                    else
                    {
                        tail = new byte[keepBytes];
                        Buffer.BlockCopy(haystack, haystackLength - keepBytes, tail, 0, keepBytes);
                    }
                }
                finally
                {
                    if (!ReferenceEquals(haystack, buffer))
                    {
                        ArrayPool<byte>.Shared.Return(haystack);
                    }
                }
            }
        }
        catch (Exception error)
        {
            throw WrapRolloutFileBusyError(error, filePath, "scan");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static bool ContainsNeedle(byte[] haystack, int haystackLength, byte[] needle)
    {
        if (needle.Length == 0)
        {
            return true;
        }

        if (haystackLength < needle.Length)
        {
            return false;
        }

        int lastStart = haystackLength - needle.Length;
        for (int index = 0; index <= lastStart; index += 1)
        {
            bool match = true;
            for (int needleIndex = 0; needleIndex < needle.Length; needleIndex += 1)
            {
                if (haystack[index + needleIndex] != needle[needleIndex])
                {
                    match = false;
                    break;
                }
            }

            if (match)
            {
                return true;
            }
        }

        return false;
    }

    private static async Task<bool> FileHasEncryptedContentAsync(string filePath, string firstLine, int startOffset)
    {
        if (firstLine.Contains("encrypted_content", StringComparison.Ordinal))
        {
            return true;
        }

        return await FileContainsTextAsync(filePath, "encrypted_content", startOffset);
    }

    private static async Task<bool> FileHasUserEventAsync(string filePath, string firstLine, int startOffset)
    {
        try
        {
            if (RecordHasUserEvent(JsonNode.Parse(firstLine)))
            {
                return true;
            }
        }
        catch
        {
            // Keep scanning the rest of the rollout below.
        }

        try
        {
            await using FileStream stream = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            if (startOffset > 0)
            {
                stream.Seek(startOffset, SeekOrigin.Begin);
            }

            using StreamReader reader = new(
                stream,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: true,
                bufferSize: 64 * 1024,
                leaveOpen: false);
            while (await reader.ReadLineAsync() is string rawLine)
            {
                if (string.IsNullOrWhiteSpace(rawLine))
                {
                    continue;
                }

                try
                {
                    if (RecordHasUserEvent(JsonNode.Parse(rawLine)))
                    {
                        return true;
                    }
                }
                catch
                {
                    // Ignore malformed non-metadata lines; provider sync only needs positive evidence.
                }
            }

            return false;
        }
        catch (Exception error)
        {
            throw WrapRolloutFileBusyError(error, filePath, "scan");
        }
    }

    private static bool RecordHasUserEvent(JsonNode? record)
    {
        if (record is not JsonObject root)
        {
            return false;
        }

        if (string.Equals(GetString(root["type"]), "event_msg", StringComparison.Ordinal)
            && root["payload"] is JsonObject eventPayload
            && string.Equals(GetString(eventPayload["type"]), "user_message", StringComparison.Ordinal))
        {
            return true;
        }

        foreach (string key in new[] { "payload", "item", "msg" })
        {
            if (root[key] is JsonObject value
                && string.Equals(GetString(value["type"]), "message", StringComparison.Ordinal)
                && string.Equals(GetString(value["role"]), "user", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static string? GetString(JsonNode? node)
    {
        try
        {
            return node?.GetValue<string>();
        }
        catch
        {
            return null;
        }
    }

    private static string ToDesktopWorkspacePath(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        string trimmed = value.Trim();
        if (trimmed.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase))
        {
            return @"\\" + trimmed[8..].Replace('/', '\\');
        }

        if (trimmed.StartsWith(@"\\?\", StringComparison.Ordinal))
        {
            string withoutPrefix = trimmed[4..].Replace('/', '\\');
            if (withoutPrefix.Length == 2 && char.IsLetter(withoutPrefix[0]) && withoutPrefix[1] == ':')
            {
                return withoutPrefix + "\\";
            }

            return withoutPrefix;
        }

        return value;
    }

    private static void TryRestoreLastWriteTimeUtc(string filePath, long? ticks)
    {
        if (ticks is null)
        {
            return;
        }

        try
        {
            File.SetLastWriteTimeUtc(filePath, new DateTime(ticks.Value, DateTimeKind.Utc));
        }
        catch
        {
            // Best effort only; rewriting metadata is still the primary operation.
        }
    }

    private static async Task<List<string>> FindLockedFilesAsync(IEnumerable<string> filePaths)
    {
        List<string> lockedPaths = [];

        foreach (string filePath in filePaths.Distinct(StringComparer.Ordinal))
        {
            try
            {
                await using FileStream stream = new(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (Exception error) when (IsRolloutFileBusyError(error))
            {
                lockedPaths.Add(filePath);
            }
        }

        lockedPaths.Sort(StringComparer.Ordinal);
        return lockedPaths;
    }

    private static bool IsRolloutFileBusyError(Exception error)
    {
        if (error.InnerException is not null && IsRolloutFileBusyError(error.InnerException))
        {
            return true;
        }

        if (error is IOException ioException)
        {
            int code = ioException.HResult & 0xFFFF;
            return code is 32 or 33 or 35;
        }

        return false;
    }

    private static bool IsRolloutFileUnreadableError(Exception error)
    {
        if (error.InnerException is not null && IsRolloutFileUnreadableError(error.InnerException))
        {
            return true;
        }

        return error is IOException or UnauthorizedAccessException;
    }

    private static Exception WrapRolloutFileBusyError(Exception error, string filePath, string action)
    {
        if (!IsRolloutFileBusyError(error))
        {
            return error;
        }

        return new IOException(
            $"Unable to {action} rollout file because it is currently in use. Close Codex and the Codex app, then retry. Locked file: {filePath}",
            error);
    }

    private readonly record struct FirstLineRecord(string FirstLine, string Separator, int Offset);
    private readonly record struct FileSnapshot(long Length, long LastWriteTimeUtcTicks);
    private readonly record struct ModelLineRewrite(
        string Line,
        bool Replaced,
        IReadOnlyList<string> OriginalModels);
    private readonly record struct ModelRewriteResult(
        int ReplacedLines,
        IReadOnlyList<TurnContextModelBackup> OriginalModels)
    {
        public static ModelRewriteResult Empty { get; } = new(0, []);
    }
}
