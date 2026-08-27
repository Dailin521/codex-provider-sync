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
        errorMessage = error.Message
    }));
    return 1;
}

static async Task<object> SyncAsync(CodexSyncService service, string codexHome, string provider)
{
    SyncResult result = await service.RunSyncAsync(codexHome, provider: provider);
    return new
    {
        schemaVersion = 1,
        ok = true,
        operation = "sync",
        result.BackupDir,
        result.TargetProvider
    };
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
    RecoveryRequiredException recovery => recovery.Code,
    SyncTransactionException transaction => transaction.Code,
    OperationCanceledException => "CANCELLED",
    _ => "RESTORE_FAILED"
};
