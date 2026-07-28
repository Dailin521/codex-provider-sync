namespace CodexProviderSync.Core;

public sealed class CodexStorageLayoutService
{
    private readonly CodexHomeService _codexHomeService;
    private readonly ConfigFileService _configFileService;

    public CodexStorageLayoutService(
        CodexHomeService? codexHomeService = null,
        ConfigFileService? configFileService = null)
    {
        _codexHomeService = codexHomeService ?? new CodexHomeService();
        _configFileService = configFileService ?? new ConfigFileService();
    }

    public CodexStorageLayout Resolve(
        string? explicitCodexHome,
        string? explicitSqliteHome,
        string configText,
        IReadOnlyDictionary<string, string?>? environment = null,
        string explicitSource = "gui")
    {
        string codexHome = _codexHomeService.NormalizeCodexHome(explicitCodexHome);
        string? configuredSqliteHome = _configFileService.ReadSqliteHomeFromConfigText(configText);
        environment ??= Environment.GetEnvironmentVariables()
            .Cast<System.Collections.DictionaryEntry>()
            .ToDictionary(
                static entry => Convert.ToString(entry.Key) ?? string.Empty,
                static entry => Convert.ToString(entry.Value),
                StringComparer.OrdinalIgnoreCase);
        environment.TryGetValue("CODEX_SQLITE_HOME", out string? environmentSqliteHome);

        (string? Value, string Source) selected = !string.IsNullOrWhiteSpace(explicitSqliteHome)
            ? (explicitSqliteHome, explicitSource)
            : !string.IsNullOrWhiteSpace(configuredSqliteHome)
                ? (configuredSqliteHome, "config")
                : !string.IsNullOrWhiteSpace(environmentSqliteHome)
                    ? (environmentSqliteHome, "env")
                    : (null, "default");

        string sqliteHome = selected.Value is null
            ? Path.Combine(codexHome, AppConstants.SqliteDirBasename)
            : Path.GetFullPath(selected.Value.Trim());
        bool allowLegacyRootFallback = string.Equals(selected.Source, "default", StringComparison.Ordinal);
        List<StateDbLocation> candidates =
        [
            new StateDbLocation(
                Path.Combine(sqliteHome, AppConstants.DbFileBasename),
                allowLegacyRootFallback
                    ? Path.Combine(AppConstants.SqliteDirBasename, AppConstants.DbFileBasename)
                    : AppConstants.DbFileBasename,
                allowLegacyRootFallback ? "sqlite-dir" : "sqlite-home")
        ];
        if (allowLegacyRootFallback)
        {
            candidates.Add(new StateDbLocation(
                Path.Combine(codexHome, AppConstants.DbFileBasename),
                AppConstants.DbFileBasename,
                "legacy-root"));
        }

        return new CodexStorageLayout
        {
            CodexHome = codexHome,
            SqliteHome = sqliteHome,
            SqliteHomeSource = selected.Source,
            AllowLegacyRootFallback = allowLegacyRootFallback,
            StateDbCandidates = candidates
        };
    }

    public CodexStorageLayout CreateDefault(string codexHome)
    {
        return Resolve(
            codexHome,
            explicitSqliteHome: null,
            configText: string.Empty,
            environment: new Dictionary<string, string?>());
    }
}
