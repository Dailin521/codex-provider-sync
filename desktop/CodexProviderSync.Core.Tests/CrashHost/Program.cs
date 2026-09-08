using System.Diagnostics;
using CodexProviderSync.Core;

string operation;
string codexHome;
string? backupDir = null;
string crashPoint;
string? failurePoint = null;
if (args is [string legacyCodexHome])
{
    operation = "sync";
    codexHome = legacyCodexHome;
    crashPoint = "after_rollout_mutation_before_applied";
}
else if (args is ["sync", string syncCodexHome, string syncPoint])
{
    operation = "sync";
    codexHome = syncCodexHome;
    crashPoint = syncPoint;
}
else if (args.Length >= 4 && args[0] == "restore-v2")
{
    operation = "restore-v2";
    codexHome = args[1];
    backupDir = args[2];
    crashPoint = args[3];
    for (var index = 4; index < args.Length; index++)
    {
        if (args[index] == "--fail-at" && index + 1 < args.Length)
        {
            failurePoint = args[++index];
        }
        else
        {
            return 64;
        }
    }
}
else
{
    return 64;
}

CodexSyncService service = new();
var failureInjected = 0;
service.FaultInjector = (observedPoint, _, _) =>
{
    if (observedPoint == failurePoint && Interlocked.Exchange(ref failureInjected, 1) == 0)
    {
        return Task.FromException(new InvalidOperationException(
            $"Forced Restore failure at {observedPoint}."));
    }
    if (observedPoint == crashPoint)
    {
        Process.GetCurrentProcess().Kill();
        Thread.Sleep(Timeout.Infinite);
    }
    return Task.CompletedTask;
};

if (operation == "sync")
{
    await service.RunSyncAsync(codexHome, provider: "openai");
}
else
{
    await service.RunRestoreAsync(codexHome, Path.GetFullPath(backupDir!));
}
return 65;
