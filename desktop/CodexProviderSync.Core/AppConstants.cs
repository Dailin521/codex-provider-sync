using System;
using System.IO;

namespace CodexProviderSync.Core;

public static class AppConstants
{
    public const string DefaultProvider = "openai";
    public const string BackupNamespace = "provider-sync";
    public const string DbFileBasename = "state_5.sqlite";
    public const int DefaultBackupRetentionCount = 5;
    public static readonly string[] SessionDirectories = ["sessions", "archived_sessions"];

    public static string DefaultCodexHome()
    {
        return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".codex");
    }

    public static string DefaultBackupRoot(string codexHome)
    {
        return Path.Combine(codexHome, "backups_state", BackupNamespace);
    }

    public static string SettingsDirectory()
    {
        return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "codex-provider-sync");
    }

    public static string SettingsPath()
    {
        return Path.Combine(SettingsDirectory(), "settings.json");
    }

    public static string LockPath(string codexHome)
    {
        return Path.Combine(codexHome, "tmp", "provider-sync.lock");
    }
}
