using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;

if (args.Length != 2)
{
    Console.Error.WriteLine("usage: HistoricalBackupProducer <core-assembly> <synthetic-codex-home>");
    return 2;
}

string assemblyPath = Path.GetFullPath(args[0]);
string codexHome = Path.GetFullPath(args[1]);
if (!File.Exists(assemblyPath)
    || !string.Equals(Path.GetExtension(assemblyPath), ".dll", StringComparison.OrdinalIgnoreCase))
{
    throw new InvalidOperationException("The historical Core assembly must be an existing DLL.");
}
if (!Directory.Exists(codexHome))
{
    throw new InvalidOperationException("The synthetic Codex Home does not exist.");
}
string assemblyDirectory = Path.GetDirectoryName(assemblyPath)
    ?? throw new InvalidOperationException("The Core assembly has no parent directory.");

AssemblyLoadContext.Default.Resolving += (_, name) =>
{
    string candidate = Path.Combine(assemblyDirectory, $"{name.Name}.dll");
    return File.Exists(candidate) ? AssemblyLoadContext.Default.LoadFromAssemblyPath(candidate) : null;
};

Assembly core = AssemblyLoadContext.Default.LoadFromAssemblyPath(assemblyPath);
Type serviceType = core.GetType("CodexProviderSync.Core.CodexSyncService", throwOnError: true)!;
object service = Activator.CreateInstance(serviceType)
    ?? throw new InvalidOperationException("Could not construct the historical CodexSyncService.");
MethodInfo method = serviceType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
    .Where(candidate => string.Equals(candidate.Name, "RunSyncAsync", StringComparison.Ordinal))
    .OrderByDescending(candidate => candidate.GetParameters().Length)
    .First();

HashSet<string> supportedParameters = new(StringComparer.Ordinal)
{
    "explicitCodexHome",
    "provider",
    "configBackupText",
    "keepCount",
    "sqliteBusyTimeoutMs",
    "model",
    "explicitSqliteHome",
    "cancellationToken"
};
string[] unknownParameters = method.GetParameters()
    .Select(parameter => parameter.Name ?? string.Empty)
    .Where(name => !supportedParameters.Contains(name))
    .ToArray();
if (unknownParameters.Length > 0)
{
    throw new InvalidOperationException(
        $"Unsupported historical RunSyncAsync parameters: {string.Join(", ", unknownParameters)}");
}

object?[] values = method.GetParameters().Select<ParameterInfo, object?>(parameter => parameter.Name switch
{
    "explicitCodexHome" => codexHome,
    "provider" => "openai",
    "configBackupText" => null,
    "keepCount" => 5,
    "sqliteBusyTimeoutMs" => null,
    "model" => null,
    "explicitSqliteHome" => null,
    "cancellationToken" => CancellationToken.None,
    _ => throw new InvalidOperationException($"Unsupported historical parameter: {parameter.Name}")
}).ToArray();

Task task = (Task)(method.Invoke(service, values)
    ?? throw new InvalidOperationException("Historical RunSyncAsync returned null."));
await task.ConfigureAwait(false);
object result = task.GetType().GetProperty("Result")?.GetValue(task)
    ?? throw new InvalidOperationException("Historical RunSyncAsync produced no result.");
string backupDir = (string?)(result.GetType().GetProperty("BackupDir")?.GetValue(result))
    ?? throw new InvalidOperationException("Historical SyncResult has no BackupDir.");

Console.WriteLine(JsonSerializer.Serialize(new
{
    schemaVersion = 1,
    backupDir,
    coreAssemblyVersion = core.GetName().Version?.ToString()
}));
return 0;
