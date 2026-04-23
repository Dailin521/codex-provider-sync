using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CodexProviderSync.Core;

public sealed class SessionRolloutService
{
    private const string StatusOnlyProvider = "__status_only__";

    public async Task<SessionChangeCollection> CollectSessionChangesAsync(
        string codexHome,
        string targetProvider,
        bool skipLockedReads = false)
    {
        List<SessionChange> changes = [];
        List<string> lockedPaths = [];
        Dictionary<string, int> sessionCounts = new(StringComparer.Ordinal);
        Dictionary<string, int> archivedCounts = new(StringComparer.Ordinal);

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

                if (!TryParseSessionMetaRecord(record.FirstLine, out JsonObject? root, out JsonObject? payload))
                {
                    continue;
                }

                string currentProvider = payload!["model_provider"]?.GetValue<string>() ?? "(missing)";
                Dictionary<string, int> bucket = dirName == "archived_sessions" ? archivedCounts : sessionCounts;
                bucket[currentProvider] = bucket.TryGetValue(currentProvider, out int count) ? count + 1 : 1;

                if (!string.Equals(targetProvider, StatusOnlyProvider, StringComparison.Ordinal)
                    && !string.Equals(currentProvider, targetProvider, StringComparison.Ordinal))
                {
                    FileSnapshot snapshot = GetFileSnapshot(rolloutPath);
                    payload["model_provider"] = targetProvider;
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
                        NormalizedLastWriteTimeUtcTicks = snapshot.LastWriteTimeUtcTicks,
                        UpdatedFirstLine = root!.ToJsonString()
                    });
                }
            }
        }

        return new SessionChangeCollection
        {
            Changes = changes,
            LockedPaths = lockedPaths.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList(),
            ProviderCounts = new ProviderCounts
            {
                Sessions = sessionCounts,
                ArchivedSessions = archivedCounts
            }
        };
    }

    public async Task<SessionApplyResult> ApplySessionChangesAsync(IEnumerable<SessionChange> changes)
    {
        int appliedCount = 0;
        List<string> appliedPaths = [];
        List<string> skippedPaths = [];

        foreach (SessionChange change in changes)
        {
            if (await TryRewriteCollectedSessionChangeAsync(change))
            {
                appliedCount += 1;
                appliedPaths.Add(change.Path);
            }
            else
            {
                skippedPaths.Add(change.Path);
            }
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
            await RewriteFirstLineAsync(
                entry.Path,
                entry.OriginalFirstLine,
                entry.OriginalSeparator,
                entry.NormalizedLastWriteTimeUtcTicks);
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
                NormalizedLastWriteTimeUtcTicks = change.NormalizedLastWriteTimeUtcTicks
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
            await using (FileStream sourceStream = OpenExclusiveRewriteStream(change.Path))
            {
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
                    change.UpdatedFirstLine,
                    change.OriginalSeparator,
                    change.OriginalOffset,
                    headerOnly: change.OriginalOffset >= change.OriginalFileLength);
            }

            ApplyNormalizedLastWriteTime(change.Path, change.NormalizedLastWriteTimeUtcTicks);
            return true;
        }
        catch (Exception error) when (IsRolloutFileBusyError(error))
        {
            return false;
        }
    }

    private async Task RewriteFirstLineAsync(
        string filePath,
        string nextFirstLine,
        string separator,
        long? normalizedLastWriteTimeUtcTicks = null)
    {
        try
        {
            await using (FileStream sourceStream = OpenExclusiveRewriteStream(filePath))
            {
                FirstLineRecord current = await ReadFirstLineRecordAsync(sourceStream);
                bool headerOnly = string.IsNullOrEmpty(current.Separator)
                    && current.Offset == Encoding.UTF8.GetByteCount(current.FirstLine);
                await RewriteFirstLineAsync(sourceStream, filePath, nextFirstLine, separator, current.Offset, headerOnly);
            }

            ApplyNormalizedLastWriteTime(filePath, normalizedLastWriteTimeUtcTicks);
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

    private static void ApplyNormalizedLastWriteTime(string filePath, long? normalizedLastWriteTimeUtcTicks)
    {
        if (normalizedLastWriteTimeUtcTicks is null or <= 0)
        {
            return;
        }

        DateTime normalizedUtc = new(normalizedLastWriteTimeUtcTicks.Value, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(filePath, normalizedUtc);
        File.SetLastAccessTimeUtc(filePath, normalizedUtc);
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
            return code is 32 or 33;
        }

        return false;
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
}
