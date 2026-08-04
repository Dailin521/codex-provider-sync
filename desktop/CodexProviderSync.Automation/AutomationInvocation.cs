using CodexProviderSync.Application;

namespace CodexProviderSync.Automation;

public enum AutomationCommand
{
    Describe,
    Status,
    Plan,
    Sync,
    Switch,
    Restore,
    Prune
}

public sealed record AutomationInvocation(
    AutomationCommand Command,
    string CommandName,
    ApplicationStatusRequest? StatusRequest,
    ApplicationWriteIntent? Intent,
    bool Apply,
    string? PlanPath,
    string? PlanDigest,
    string? LedgerRoot,
    TimeSpan? Timeout)
{
    public bool IsWrite => Command is
        AutomationCommand.Plan or
        AutomationCommand.Sync or
        AutomationCommand.Switch or
        AutomationCommand.Restore or
        AutomationCommand.Prune;
}

public sealed record AutomationParseResult(
    AutomationInvocation? Invocation,
    AutomationProtocolResponse? Error)
{
    public bool IsSuccess => Invocation is not null;
}
