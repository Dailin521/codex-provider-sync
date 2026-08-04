namespace CodexProviderSync.Application;

/// <summary>
/// Process-local ledger for the desktop GUI. The one-shot Business Automation
/// host must use a durable <see cref="IApplicationPlanLedger"/> instead.
/// </summary>
public sealed class InMemoryApplicationPlanLedger : IApplicationPlanLedger
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);

    public Task RegisterAsync(
        ApplicationOperationPlan plan,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_entries.TryAdd(plan.PlanId, new Entry(plan.Digest)))
            {
                throw new InvalidOperationException($"Plan {plan.PlanId} is already registered.");
            }
        }

        return Task.CompletedTask;
    }

    public Task<ApplicationPlanClaimResult> TryClaimAsync(
        string planId,
        string digest,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ApplicationPlanClaimStatus status;
        lock (_gate)
        {
            if (!_entries.TryGetValue(planId, out Entry? entry))
            {
                status = ApplicationPlanClaimStatus.NotFound;
            }
            else if (!string.Equals(entry.Digest, digest, StringComparison.Ordinal))
            {
                status = ApplicationPlanClaimStatus.DigestMismatch;
            }
            else if (entry.Claimed)
            {
                status = ApplicationPlanClaimStatus.AlreadyUsed;
            }
            else
            {
                entry.Claimed = true;
                status = ApplicationPlanClaimStatus.Claimed;
            }
        }

        return Task.FromResult(new ApplicationPlanClaimResult(status));
    }

    public Task CompleteAsync(
        string planId,
        ApplicationOperationLifecycle lifecycle,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureTerminalLifecycle(lifecycle);
        lock (_gate)
        {
            if (!_entries.TryGetValue(planId, out Entry? entry) || !entry.Claimed)
            {
                throw new InvalidOperationException($"Plan {planId} has not been claimed.");
            }

            if (entry.TerminalLifecycle is { } existing)
            {
                if (existing != lifecycle)
                {
                    throw new InvalidOperationException(
                        $"Plan {planId} is already completed as {existing} and cannot transition to {lifecycle}.");
                }

                return Task.CompletedTask;
            }

            entry.TerminalLifecycle = lifecycle;
        }

        return Task.CompletedTask;
    }

    private static void EnsureTerminalLifecycle(ApplicationOperationLifecycle lifecycle)
    {
        if (lifecycle is not (
            ApplicationOperationLifecycle.Succeeded
            or ApplicationOperationLifecycle.Failed
            or ApplicationOperationLifecycle.Cancelled
            or ApplicationOperationLifecycle.Rejected
            or ApplicationOperationLifecycle.RecoveryRequired))
        {
            throw new ArgumentOutOfRangeException(
                nameof(lifecycle),
                lifecycle,
                "Only terminal Application lifecycles can complete a claimed plan.");
        }
    }

    private sealed class Entry(string digest)
    {
        public string Digest { get; } = digest;

        public bool Claimed { get; set; }

        public ApplicationOperationLifecycle? TerminalLifecycle { get; set; }
    }
}
