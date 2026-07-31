using System.Runtime.CompilerServices;

namespace CodexProviderSync.Core.Tests;

internal static class TestEnvironment
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        Environment.SetEnvironmentVariable("CODEX_SQLITE_HOME", null);
    }
}
