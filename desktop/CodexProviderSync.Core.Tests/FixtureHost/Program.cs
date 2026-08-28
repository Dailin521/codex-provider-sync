using System.Text.Json;
using CodexProviderSync.Core;

if (args.Length < 2)
{
    return 64;
}

string operation = args[0];
string codexHome = Path.GetFullPath(args[1]);
CodexSyncService service = new();

try
{
    object output = operation switch
    {
        "sync" when args.Length == 3 => await SyncAsync(service, codexHome, args[2]),
        "sync-explicit" when args.Length == 4 => await SyncAsync(
            service,
            codexHome,
            args[2],
            Path.GetFullPath(args[3])),
        "sync-gated" when args.Length == 6 => await GatedSyncAsync(
            service,
            codexHome,
            args[2],
            Path.GetFullPath(args[3]),
            Path.GetFullPath(args[4]),
            Path.GetFullPath(args[5])),
        "restore" or "restore-v2" when args.Length == 3 => await RestoreAsync(service, codexHome, args[2]),
        "source-identity" when args.Length == 3 => await SourceIdentityAsync(args[2]),
        _ => throw new ArgumentException("Unsupported fixture operation.")
    };
    Console.WriteLine(JsonSerializer.Serialize(output));
    return 0;
}
catch (Exception error)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(new
    {
        schemaVersion = 1,
        ok = false,
        errorType = error.GetType().Name,
        errorCode = ErrorCode(error),
        errorMessage = error.Message,
        busyScope = BusyScope(error)
    }));
    return 1;
}

static async Task<object> SyncAsync(
    CodexSyncService service,
    string codexHome,
    string provider,
    string? explicitSqliteHome = null)
{
    SyncResult result = await service.RunSyncAsync(
        codexHome,
        provider: provider,
        explicitSqliteHome: explicitSqliteHome);
    return new
    {
        schemaVersion = 1,
        ok = true,
        operation = "sync",
        result.BackupDir,
        result.TargetProvider
    };
}

static async Task<object> GatedSyncAsync(
    CodexSyncService service,
    string codexHome,
    string provider,
    string explicitSqliteHome,
    string readyPath,
    string releasePath)
{
    service.FaultInjector = async (point, _, _) =>
    {
        if (point != "before_backup")
        {
            return;
        }
        Directory.CreateDirectory(Path.GetDirectoryName(readyPath)!);
        await using (FileStream marker = new(
            readyPath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 1,
            useAsync: true))
        {
            await marker.WriteAsync("ready\n"u8.ToArray());
            await marker.FlushAsync();
        }
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (File.Exists(releasePath))
            {
                return;
            }
            await Task.Delay(25);
        }
        throw new TimeoutException("Timed out waiting for the cross-runtime writer release marker.");
    };
    return await SyncAsync(service, codexHome, provider, explicitSqliteHome);
}

static async Task<object> RestoreAsync(CodexSyncService service, string codexHome, string backupDir)
{
    RestoreResult result = await service.RunRestoreAsync(codexHome, Path.GetFullPath(backupDir));
    return new
    {
        schemaVersion = 1,
        ok = true,
        operation = "restore",
        result.BackupDir,
        result.TargetProvider,
        result.RestoreVersion,
        result.RestoreOperationId,
        result.PreRestoreSnapshotId,
        result.RestoreJournalState,
        result.CommitAcknowledgementRecovered,
        result.ResolvedOperationIds
    };
}

static async Task<object> SourceIdentityAsync(string backupDir)
{
    RestoreBackupIdentity identity = await RestoreV2Service.CaptureSourceIdentityAsync(
        Path.GetFullPath(backupDir));
    return new
    {
        schemaVersion = 1,
        ok = true,
        operation = "source-identity",
        identity.BackupId,
        identity.BackupDir,
        identity.Revision
    };
}

static string ErrorCode(Exception error) => error switch
{
    _ when LockService.IsOperationBusy(error) => "OPERATION_BUSY",
    RecoveryRequiredException recovery => recovery.Code,
    SyncTransactionException transaction => transaction.Code,
    OperationCanceledException => "CANCELLED",
    _ => "RESTORE_FAILED"
};

static string? BusyScope(Exception error)
{
    if (error.Data["codex-provider-sync/lock-scope"] is string scope)
    {
        return scope;
    }
    if (error is AggregateException aggregate)
    {
        return aggregate.InnerExceptions.Select(BusyScope).FirstOrDefault(value => value is not null);
    }
    return error.InnerException is null ? null : BusyScope(error.InnerException);
}
