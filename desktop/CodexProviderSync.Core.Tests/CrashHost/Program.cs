using System.Diagnostics;
using CodexProviderSync.Core;

if (args is not [string codexHome])
{
    return 64;
}

CodexSyncService service = new();
service.FaultInjector = (point, _, _) =>
{
    if (point == "after_rollout_mutation_before_applied")
    {
        Process.GetCurrentProcess().Kill();
        Thread.Sleep(Timeout.Infinite);
    }
    return Task.CompletedTask;
};

await service.RunSyncAsync(codexHome, provider: "openai");
return 65;
