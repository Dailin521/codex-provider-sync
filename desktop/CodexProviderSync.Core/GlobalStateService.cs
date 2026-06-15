using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;

namespace CodexProviderSync.Core;

public sealed class GlobalStateService
{
    private readonly SqliteStateService _sqliteStateService = new();

    public string StatePath(string codexHome)
    {
        return Path.Combine(codexHome, AppConstants.GlobalStateFileBasename);
    }

    public string BackupPath(string codexHome)
    {
        return Path.Combine(codexHome, AppConstants.GlobalStateBackupFileBasename);
    }

    public async Task<IReadOnlyList<ThreadCwdStat>> ReadThreadCwdStatsAsync(string codexHome)
    {
        string? dbPath = _sqliteStateService.StateDbPaths(codexHome).LastOrDefault();
        if (dbPath is null)
        {
            return [];
        }

        try
        {
            await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadOnly);
            await connection.OpenAsync();
            HashSet<string> columns = await ReadThreadTableColumnsAsync(connection);
            if (!columns.Contains("cwd"))
            {
                return [];
            }

            string updatedAtExpression = columns.Contains("updated_at_ms")
                ? (columns.Contains("updated_at")
                    ? "COALESCE(MAX(updated_at_ms), MAX(updated_at) * 1000, 0)"
                    : "COALESCE(MAX(updated_at_ms), 0)")
                : (columns.Contains("updated_at")
                    ? "COALESCE(MAX(updated_at) * 1000, 0)"
                    : "0");

            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT
                  cwd,
                  COUNT(*) AS count,
                  {updatedAtExpression} AS updated_at_ms
                FROM threads
                WHERE cwd IS NOT NULL AND cwd <> ''
                GROUP BY cwd
                ORDER BY count DESC, updated_at_ms DESC, cwd
                """;

            List<ThreadCwdStat> rows = [];
            await using SqliteDataReader reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                string cwd = reader.GetString(0);
                string? normalized = NormalizeComparablePath(cwd);
                if (string.IsNullOrWhiteSpace(normalized))
                {
                    continue;
                }

                rows.Add(new ThreadCwdStat
                {
                    Cwd = cwd,
                    NormalizedCwd = normalized,
                    Count = reader.GetInt64(1),
                    UpdatedAtMs = reader.GetInt64(2)
                });
            }

            return rows;
        }
        catch (Exception error)
        {
            throw SqliteStateService.WrapSqliteMalformedError(
                SqliteStateService.WrapSqliteBusyError(error, "update session provider metadata"),
                "update session provider metadata");
        }
    }

    public async Task<WorkspaceRootSyncResult> SyncWorkspaceRootsAsync(
        string codexHome,
        IReadOnlyList<ThreadCwdStat>? cwdStats = null)
    {
        string statePath = StatePath(codexHome);
        if (!File.Exists(statePath))
        {
            return new WorkspaceRootSyncResult
            {
                Present = false,
                Updated = false,
                UpdatedWorkspaceRoots = 0,
                SavedWorkspaceRootCount = 0
            };
        }

        JsonObject state = JsonNode.Parse(await File.ReadAllTextAsync(statePath))?.AsObject()
            ?? throw new InvalidOperationException($"Global state file is invalid: {statePath}");
        IReadOnlyList<ThreadCwdStat> effectiveCwdStats = cwdStats ?? await ReadThreadCwdStatsAsync(codexHome);

        List<string> existingSavedRoots = ToPathList(state["electron-saved-workspace-roots"]);
        List<string> existingProjectOrder = ToPathList(state["project-order"]);
        List<string> existingActiveRoots = ToPathList(state["active-workspace-roots"]);
        JsonNode? originalActiveRoots = state["active-workspace-roots"]?.DeepClone();
        JsonNode? originalLabels = state["electron-workspace-root-labels"]?.DeepClone();
        JsonNode? originalOpenTargets = state["open-in-target-preferences"]?.DeepClone();

        IEnumerable<string> savedRootCandidates = existingProjectOrder.Count > 0
            ? existingProjectOrder.Concat(existingSavedRoots).Concat(existingActiveRoots)
            : existingSavedRoots.Concat(existingActiveRoots);
        List<string> nextSavedRoots = DedupePaths(
            savedRootCandidates.Select(value => ResolveStoredPath(value, effectiveCwdStats)));
        IEnumerable<string> projectOrderCandidates = existingProjectOrder.Count > 0
            ? existingProjectOrder.Concat(existingSavedRoots)
            : nextSavedRoots;
        List<string> nextProjectOrder = DedupePaths(
            projectOrderCandidates.Select(value => ResolveStoredPath(value, effectiveCwdStats)));
        List<string> nextActiveRoots = DedupePaths(existingActiveRoots.Select(value => ResolveStoredPath(value, effectiveCwdStats)));

        JsonNode? nextActiveRootsNode = state["active-workspace-roots"] is JsonArray
            ? ToJsonArray(nextActiveRoots)
            : (nextActiveRoots.Count > 0 ? JsonValue.Create(nextActiveRoots[0]) : null);
        JsonObject? nextLabels = CopyResolvedObjectKeys(state["electron-workspace-root-labels"] as JsonObject, effectiveCwdStats);
        JsonObject? nextOpenTargets = state["open-in-target-preferences"] as JsonObject;
        if (nextOpenTargets is not null)
        {
            nextOpenTargets = (JsonObject)nextOpenTargets.DeepClone();
            if (nextOpenTargets["perPath"] is JsonObject perPath)
            {
                nextOpenTargets["perPath"] = CopyResolvedObjectKeys(perPath, effectiveCwdStats);
            }
        }

        bool savedRootsChanged = !existingSavedRoots.SequenceEqual(nextSavedRoots, StringComparer.Ordinal);
        bool projectOrderChanged = !existingProjectOrder.SequenceEqual(nextProjectOrder, StringComparer.Ordinal);
        bool activeRootsChanged = !JsonNode.DeepEquals(originalActiveRoots, nextActiveRootsNode);
        bool labelsChanged = !JsonNode.DeepEquals(originalLabels, nextLabels);
        bool openTargetsChanged = !JsonNode.DeepEquals(originalOpenTargets, nextOpenTargets);
        bool backupMissing = !File.Exists(BackupPath(codexHome));

        state["electron-saved-workspace-roots"] = ToJsonArray(nextSavedRoots);
        state["project-order"] = ToJsonArray(nextProjectOrder);
        state["active-workspace-roots"] = nextActiveRootsNode;
        if (nextLabels is not null)
        {
            state["electron-workspace-root-labels"] = nextLabels;
        }
        if (nextOpenTargets is not null)
        {
            state["open-in-target-preferences"] = nextOpenTargets;
        }

        bool updated = savedRootsChanged || projectOrderChanged || activeRootsChanged || labelsChanged || openTargetsChanged || backupMissing;
        if (updated)
        {
            string json = state.ToJsonString(JsonOptions()) + Environment.NewLine;
            await File.WriteAllTextAsync(statePath, json);
            await File.WriteAllTextAsync(BackupPath(codexHome), json);
        }

        return new WorkspaceRootSyncResult
        {
            Present = true,
            Updated = updated,
            UpdatedWorkspaceRoots = CountArrayChanges(existingSavedRoots, nextSavedRoots),
            SavedWorkspaceRootCount = nextSavedRoots.Count
        };
    }

    public async Task<IReadOnlyList<ProjectThreadVisibility>> ReadProjectThreadVisibilityAsync(
        string codexHome,
        int pageSize = 50)
    {
        string statePath = StatePath(codexHome);
        if (!File.Exists(statePath))
        {
            return [];
        }

        JsonObject state = JsonNode.Parse(await File.ReadAllTextAsync(statePath))?.AsObject()
            ?? throw new InvalidOperationException($"Global state file is invalid: {statePath}");
        List<string> roots = ReadWorkspaceRootsFromState(state);
        if (roots.Count == 0)
        {
            return [];
        }

        string? dbPath = _sqliteStateService.StateDbPaths(codexHome).LastOrDefault();
        if (dbPath is null)
        {
            return roots
                .Select(static root => new ProjectThreadVisibility
                {
                    Root = root,
                    InteractiveThreads = 0,
                    FirstPageThreads = 0,
                    ExactCwdMatches = 0,
                    VerbatimCwdRows = 0,
                    Ranks = [],
                    RankPreview = string.Empty,
                    ProviderCounts = new Dictionary<string, int>(StringComparer.Ordinal)
                })
                .ToList();
        }

        try
        {
            await using SqliteConnection connection = OpenConnection(dbPath, SqliteOpenMode.ReadOnly);
            await connection.OpenAsync();
            HashSet<string> columns = await ReadThreadTableColumnsAsync(connection);
            if (!columns.Contains("cwd"))
            {
                return [];
            }

            string sourceFilter = columns.Contains("source") ? "AND source IN ('cli', 'vscode')" : string.Empty;
            string archivedFilter = columns.Contains("archived") ? "AND archived = 0" : string.Empty;
            string firstUserFilter = columns.Contains("first_user_message") ? "AND first_user_message <> ''" : string.Empty;
            string providerExpression = columns.Contains("model_provider") ? "model_provider" : "'' AS model_provider";
            string timeExpression = BuildTimeExpression(columns);

            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT
                  id,
                  cwd,
                  {providerExpression},
                  {timeExpression} AS sort_ts
                FROM threads
                WHERE cwd IS NOT NULL AND cwd <> ''
                  {archivedFilter}
                  {firstUserFilter}
                  {sourceFilter}
                ORDER BY sort_ts DESC, id DESC
                """;

            List<(string Cwd, string DesktopCwd, string? NormalizedCwd, string Provider, int Rank)> rows = [];
            await using SqliteDataReader reader = await command.ExecuteReaderAsync();
            int rank = 1;
            while (await reader.ReadAsync())
            {
                string cwd = reader.GetString(1);
                string provider = reader.IsDBNull(2) || string.IsNullOrWhiteSpace(reader.GetString(2))
                    ? "(missing)"
                    : reader.GetString(2);
                rows.Add((cwd, ToDesktopWorkspacePath(cwd), NormalizeComparablePath(cwd), provider, rank));
                rank += 1;
            }

            List<ProjectThreadVisibility> result = [];
            foreach (string root in roots)
            {
                string exactRoot = ToDesktopWorkspacePath(root);
                string? normalizedRoot = NormalizeComparablePath(root);
                List<(string Cwd, string DesktopCwd, string? NormalizedCwd, string Provider, int Rank)> matchingRows = rows
                    .Where(row => string.Equals(row.NormalizedCwd, normalizedRoot, StringComparison.Ordinal))
                    .ToList();
                List<int> ranks = matchingRows.Select(static row => row.Rank).ToList();
                Dictionary<string, int> providerCounts = new(StringComparer.Ordinal);
                foreach (var (_, _, _, provider, _) in matchingRows)
                {
                    providerCounts[provider] = providerCounts.GetValueOrDefault(provider) + 1;
                }

                result.Add(new ProjectThreadVisibility
                {
                    Root = exactRoot,
                    InteractiveThreads = matchingRows.Count,
                    FirstPageThreads = ranks.Count(value => value <= pageSize),
                    ExactCwdMatches = matchingRows.Count(row => string.Equals(row.Cwd, exactRoot, StringComparison.Ordinal)),
                    VerbatimCwdRows = matchingRows.Count(row => row.Cwd.StartsWith(@"\\?\", StringComparison.Ordinal)),
                    Ranks = ranks,
                    RankPreview = FormatRankPreview(ranks),
                    ProviderCounts = providerCounts
                });
            }

            return result;
        }
        catch (Exception error)
        {
            throw SqliteStateService.WrapSqliteMalformedError(
                SqliteStateService.WrapSqliteBusyError(error, "read project thread visibility diagnostics"),
                "read project thread visibility diagnostics");
        }
    }

    private static async Task<HashSet<string>> ReadThreadTableColumnsAsync(SqliteConnection connection)
    {
        HashSet<string> columns = new(StringComparer.Ordinal);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "PRAGMA table_info(\"threads\")";
        await using SqliteDataReader reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(1));
        }

        return columns;
    }

    private static string BuildTimeExpression(HashSet<string> columns)
    {
        List<string> expressions = [];
        if (columns.Contains("updated_at_ms"))
        {
            expressions.Add("updated_at_ms");
        }
        if (columns.Contains("updated_at"))
        {
            expressions.Add("updated_at * 1000");
        }
        if (columns.Contains("created_at_ms"))
        {
            expressions.Add("created_at_ms");
        }
        if (columns.Contains("created_at"))
        {
            expressions.Add("created_at * 1000");
        }
        expressions.Add("0");
        return $"COALESCE({string.Join(", ", expressions)})";
    }

    private static List<string> ReadWorkspaceRootsFromState(JsonObject state)
    {
        List<string> savedRoots = ToPathList(state["electron-saved-workspace-roots"]);
        List<string> projectOrder = ToPathList(state["project-order"]);
        List<string> activeRoots = ToPathList(state["active-workspace-roots"]);
        IEnumerable<string> candidates = projectOrder.Count > 0
            ? projectOrder.Concat(savedRoots).Concat(activeRoots)
            : savedRoots.Concat(activeRoots);
        return DedupePaths(candidates.Select(ToDesktopWorkspacePath));
    }

    private static string FormatRankPreview(IReadOnlyList<int> ranks, int maxCount = 12)
    {
        string preview = string.Join(", ", ranks.Take(maxCount));
        int remaining = ranks.Count - Math.Min(ranks.Count, maxCount);
        return remaining > 0 ? $"{preview} (+{remaining} more)" : preview;
    }

    private static SqliteConnection OpenConnection(string dbPath, SqliteOpenMode mode)
    {
        SqliteConnectionStringBuilder builder = new()
        {
            DataSource = dbPath,
            Mode = mode,
            Pooling = false
        };
        return new SqliteConnection(builder.ConnectionString);
    }

    private static string ResolveStoredPath(string value, IReadOnlyList<ThreadCwdStat> cwdStats)
    {
        string? comparable = NormalizeComparablePath(value);
        if (string.IsNullOrWhiteSpace(comparable))
        {
            return value;
        }

        ThreadCwdStat? match = cwdStats
            .Where(entry => string.Equals(entry.NormalizedCwd, comparable, StringComparison.Ordinal))
            .OrderByDescending(entry => entry.Count)
            .ThenByDescending(entry => entry.UpdatedAtMs)
            .ThenBy(entry => entry.Cwd, StringComparer.Ordinal)
            .FirstOrDefault();

        return ToDesktopWorkspacePath(match?.Cwd ?? value);
    }

    private static List<string> ToPathList(JsonNode? node)
    {
        if (node is JsonArray array)
        {
            return array
                .Select(static entry => entry?.GetValue<string>())
                .Where(static entry => !string.IsNullOrWhiteSpace(entry))
                .Cast<string>()
                .ToList();
        }

        if (node is JsonValue value)
        {
            string? text = value.GetValue<string>();
            return string.IsNullOrWhiteSpace(text) ? [] : [text];
        }

        return [];
    }

    private static JsonArray ToJsonArray(IEnumerable<string> values)
    {
        JsonArray array = [];
        foreach (string value in values)
        {
            array.Add(value);
        }

        return array;
    }

    private static List<string> DedupePaths(IEnumerable<string> values)
    {
        HashSet<string> seen = new(StringComparer.Ordinal);
        List<string> result = [];
        foreach (string value in values)
        {
            string? comparable = NormalizeComparablePath(value);
            if (string.IsNullOrWhiteSpace(comparable) || !seen.Add(comparable))
            {
                continue;
            }

            result.Add(value);
        }

        return result;
    }

    private static JsonObject? CopyResolvedObjectKeys(JsonObject? source, IReadOnlyList<ThreadCwdStat> cwdStats)
    {
        if (source is null)
        {
            return null;
        }

        JsonObject result = [];
        foreach ((string key, JsonNode? value) in source)
        {
            string resolved = ResolveStoredPath(key, cwdStats);
            if (!result.ContainsKey(resolved) || string.Equals(resolved, key, StringComparison.Ordinal))
            {
                result[resolved] = value?.DeepClone();
            }
        }

        return result;
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

    private static string? NormalizeComparablePath(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        string normalized = value.Trim();
        if (normalized.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase))
        {
            normalized = @"\\" + normalized[8..];
        }
        else if (normalized.StartsWith(@"\\?\", StringComparison.Ordinal))
        {
            normalized = normalized[4..];
        }

        normalized = normalized.Replace('/', '\\').TrimEnd('\\');
        if (normalized.Length == 2 && char.IsLetter(normalized[0]) && normalized[1] == ':')
        {
            normalized += "\\";
        }

        return normalized.ToLowerInvariant();
    }

    private static int CountArrayChanges(IReadOnlyList<string> previous, IReadOnlyList<string> next)
    {
        int compared = Math.Max(previous.Count, next.Count);
        int changed = 0;
        for (int index = 0; index < compared; index += 1)
        {
            string? left = index < previous.Count ? previous[index] : null;
            string? right = index < next.Count ? next[index] : null;
            if (!string.Equals(left, right, StringComparison.Ordinal))
            {
                changed += 1;
            }
        }

        return changed;
    }

    private static JsonSerializerOptions JsonOptions()
    {
        return new JsonSerializerOptions
        {
            WriteIndented = true
        };
    }
}
