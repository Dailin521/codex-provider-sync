using CodexProviderSync.Application;

if (args is not [string ledgerRoot, string planId, string digest, string readyPath, string startPath])
{
    return 64;
}
if (!Path.IsPathFullyQualified(ledgerRoot)
    || !Path.IsPathFullyQualified(readyPath)
    || !Path.IsPathFullyQualified(startPath))
{
    return 65;
}

await File.WriteAllTextAsync(readyPath, $"{Environment.ProcessId}\n");
using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
while (!File.Exists(startPath))
{
    await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
}

FileApplicationPlanLedger ledger = new(ledgerRoot);
ApplicationPlanClaimResult result = await ledger.TryClaimAsync(
    planId,
    digest,
    timeout.Token);
Console.WriteLine(result.Status);
return 0;
