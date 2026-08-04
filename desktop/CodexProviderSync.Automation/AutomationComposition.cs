using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.Automation;

/// <summary>
/// Production composition boundary. The GUI and this host both consume the
/// same IApplicationService use cases. All writes flow through Core's checked
/// snapshot/execution boundary.
/// </summary>
public sealed class AutomationApplicationFactory : IAutomationApplicationFactory
{
    private readonly Func<IApplicationStatusPort> _statusPortFactory;
    private readonly Func<IApplicationWritePort> _writePortFactory;

    public AutomationApplicationFactory(
        Func<IApplicationStatusPort>? statusPortFactory = null,
        Func<IApplicationWritePort>? writePortFactory = null)
    {
        _statusPortFactory = statusPortFactory ?? (static () => new CoreApplicationStatusPort());
        _writePortFactory = writePortFactory ?? (static () => new CoreApplicationWritePort());
    }

    public IApplicationService Create(AutomationInvocation invocation)
    {
        ArgumentNullException.ThrowIfNull(invocation);
        IApplicationPlanLedger ledger = invocation.IsWrite
            ? new FileApplicationPlanLedger(invocation.LedgerRoot!)
            : new InMemoryApplicationPlanLedger();
        return new ApplicationService(
            _statusPortFactory(),
            _writePortFactory(),
            ledger);
    }

}
