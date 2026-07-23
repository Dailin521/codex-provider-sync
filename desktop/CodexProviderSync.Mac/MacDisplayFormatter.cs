using CodexProviderSync.Core;

namespace CodexProviderSync.Mac;

internal static class MacDisplayFormatter
{
    public static string FormatStatus(StatusSnapshot status, string language) =>
        TextFormatter.FormatStatus(status, language);

    public static string FormatSyncResult(SyncResult result, string label, string language) =>
        TextFormatter.FormatSyncResult(result, label, language);

    public static string FormatRestoreResult(RestoreResult result, string language) =>
        TextFormatter.FormatRestoreResult(result, language);

    public static string FormatBackupPruneResult(BackupPruneResult result, string language) =>
        TextFormatter.FormatBackupPruneResult(result, language);

    public static string FormatProviderSources(ProviderOption option, string language) =>
        TextFormatter.FormatProviderSources(option, language);
}
