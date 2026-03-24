using System.IO;
using System.Threading.Tasks;

namespace CodexProviderSync.Core;

public sealed class CodexHomeService
{
    public string NormalizeCodexHome(string? explicitCodexHome)
    {
        return Path.GetFullPath(
            string.IsNullOrWhiteSpace(explicitCodexHome)
                ? AppConstants.DefaultCodexHome()
                : explicitCodexHome.Trim());
    }

    public Task EnsureCodexHomeAsync(string codexHome)
    {
        if (!Directory.Exists(codexHome))
        {
            throw new DirectoryNotFoundException($"Codex home was not found: {codexHome}");
        }

        return Task.CompletedTask;
    }

    public string ConfigPath(string codexHome)
    {
        return Path.Combine(codexHome, "config.toml");
    }

    public string BackupRoot(string codexHome)
    {
        return AppConstants.DefaultBackupRoot(codexHome);
    }
}
