using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.App;

internal sealed record ApplicationOperationTraceRecord(
    string OperationId,
    ApplicationOperationKind Operation,
    ApplicationOperationLifecycle Lifecycle,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc,
    IReadOnlyList<string> ErrorCodes);

internal static class GuiApplicationOutcomePolicy
{
    internal static bool IsAppliedSuccess<T>(
        ApplicationOutcome<ApplicationWriteResult<T>> outcome)
        where T : class =>
        outcome.Lifecycle == ApplicationOperationLifecycle.Succeeded
        && outcome.Data is { Applied: true, Result: not null };

    internal static bool IsPlanStale<T>(
        ApplicationOutcome<ApplicationWriteResult<T>> outcome)
        where T : class =>
        outcome.Errors.Any(
            static error => string.Equals(error.Code, "plan_stale", StringComparison.Ordinal));
}

/// <summary>
/// Carries the causal GUI invocation across WinForms async-event continuations.
/// The bridge opens a window immediately around the real control event and the
/// event handler closes it only after all Application work and the follow-up
/// refresh have completed.
/// </summary>
internal sealed class ApplicationOperationTraceHub
{
    private readonly AsyncLocal<InvocationCorrelation?> _current = new();

    internal ApplicationInvocationScope BeginInvocation(string requestId, string automationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        ArgumentException.ThrowIfNullOrWhiteSpace(automationId);
        InvocationCorrelation? previous = _current.Value;
        ApplicationInvocationWindow window = new(requestId, automationId);
        InvocationCorrelation correlation = new(window);
        _current.Value = correlation;
        return new ApplicationInvocationScope(this, correlation, previous, window);
    }

    internal void Publish<T>(ApplicationOutcome<T> outcome)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(outcome);
        InvocationCorrelation? correlation = _current.Value;
        if (correlation is null)
        {
            return;
        }

        _ = correlation.Window.TryAdd(new ApplicationOperationTraceRecord(
            outcome.OperationId,
            outcome.Operation,
            outcome.Lifecycle,
            outcome.StartedAtUtc,
            outcome.CompletedAtUtc,
            Array.AsReadOnly(outcome.Errors.Select(static error => error.Code).ToArray())));
    }

    internal void CompleteCurrentInvocation()
    {
        _current.Value?.Window.Complete();
    }

    private void Restore(
        InvocationCorrelation correlation,
        InvocationCorrelation? previous)
    {
        if (ReferenceEquals(_current.Value, correlation))
        {
            _current.Value = previous;
        }
    }

    internal sealed record InvocationCorrelation(ApplicationInvocationWindow Window);

    internal sealed class ApplicationInvocationScope : IDisposable
    {
        private readonly ApplicationOperationTraceHub _owner;
        private readonly InvocationCorrelation _correlation;
        private readonly InvocationCorrelation? _previous;
        private int _disposed;

        internal ApplicationInvocationScope(
            ApplicationOperationTraceHub owner,
            InvocationCorrelation correlation,
            InvocationCorrelation? previous,
            ApplicationInvocationWindow window)
        {
            _owner = owner;
            _correlation = correlation;
            _previous = previous;
            Window = window;
        }

        internal ApplicationInvocationWindow Window { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _owner.Restore(_correlation, _previous);
            }
        }
    }
}

internal sealed class ApplicationInvocationWindow
{
    private readonly object _gate = new();
    private readonly List<ApplicationOperationTraceRecord> _operations = [];
    private readonly TaskCompletionSource<IReadOnlyList<ApplicationOperationTraceRecord>> _completion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _completed;

    internal ApplicationInvocationWindow(string requestId, string automationId)
    {
        RequestId = requestId;
        AutomationId = automationId;
    }

    internal string RequestId { get; }

    internal string AutomationId { get; }

    internal bool TryAdd(ApplicationOperationTraceRecord operation)
    {
        lock (_gate)
        {
            if (_completed)
            {
                // Trace observation must never turn an already-completed Core
                // operation into an application failure.
                return false;
            }
            _operations.Add(operation);
            return true;
        }
    }

    internal void Complete()
    {
        IReadOnlyList<ApplicationOperationTraceRecord> snapshot;
        lock (_gate)
        {
            if (_completed)
            {
                return;
            }
            _completed = true;
            snapshot = Array.AsReadOnly(_operations.ToArray());
        }
        _completion.TrySetResult(snapshot);
    }

    internal Task<IReadOnlyList<ApplicationOperationTraceRecord>> WaitAsync(
        CancellationToken cancellationToken) =>
        _completion.Task.WaitAsync(cancellationToken);
}

internal sealed class TrackedApplicationService : IApplicationService
{
    private readonly IApplicationService _inner;
    private readonly ApplicationOperationTraceHub _trace;

    internal TrackedApplicationService(
        IApplicationService inner,
        ApplicationOperationTraceHub trace)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _trace = trace ?? throw new ArgumentNullException(nameof(trace));
    }

    public Task<ApplicationOutcome<ApplicationDescription>> DescribeAsync(
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.DescribeAsync(cancellationToken));

    public Task<ApplicationOutcome<StatusSnapshot>> GetStatusAsync(
        ApplicationStatusRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.GetStatusAsync(request, cancellationToken));

    public Task<ApplicationOutcome<ApplicationOperationPlan>> CreatePlanAsync(
        CreateApplicationPlanRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.CreatePlanAsync(request, cancellationToken));

    public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SyncAsync(
        SyncApplicationRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.SyncAsync(request, cancellationToken));

    public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SwitchAsync(
        SwitchApplicationRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.SwitchAsync(request, cancellationToken));

    public Task<ApplicationOutcome<ApplicationWriteResult<RestoreResult>>> RestoreAsync(
        RestoreApplicationRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.RestoreAsync(request, cancellationToken));

    public Task<ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>>> PruneAsync(
        PruneApplicationRequest request,
        CancellationToken cancellationToken = default) =>
        TrackAsync(() => _inner.PruneAsync(request, cancellationToken));

    private async Task<ApplicationOutcome<T>> TrackAsync<T>(
        Func<Task<ApplicationOutcome<T>>> execute)
        where T : class
    {
        ApplicationOutcome<T> outcome = await execute().ConfigureAwait(false);
        _trace.Publish(outcome);
        return outcome;
    }
}
