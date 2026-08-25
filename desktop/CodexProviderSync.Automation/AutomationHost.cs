using System.Text.Json;
using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.Automation;

public interface IAutomationApplicationFactory
{
    IApplicationService Create(AutomationInvocation invocation);
}

public sealed class AutomationHost
{
    private readonly IAutomationApplicationFactory _applicationFactory;

    public AutomationHost(IAutomationApplicationFactory applicationFactory)
    {
        _applicationFactory = applicationFactory ?? throw new ArgumentNullException(nameof(applicationFactory));
    }

    public async Task<AutomationRunResult> RunAsync(
        IReadOnlyList<string> args,
        CancellationToken cancellationToken = default)
    {
        AutomationParseResult parsed = AutomationCommandLine.Parse(args);
        if (!parsed.IsSuccess)
        {
            AutomationProtocolResponse response = parsed.Error!;
            return new AutomationRunResult(response.ExitCode, response, response.Errors[0].Message);
        }

        AutomationInvocation invocation = parsed.Invocation!;
        using CancellationTokenSource? timeout = invocation.Timeout is null
            ? null
            : new CancellationTokenSource(invocation.Timeout.Value);
        using CancellationTokenSource linked = timeout is null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);

        try
        {
            IApplicationService service = _applicationFactory.Create(invocation);
            return await DispatchAsync(
                service,
                invocation,
                timeout,
                cancellationToken,
                linked.Token);
        }
        catch (PlanDocumentException error)
        {
            AutomationProtocolResponse response = new(
                ApplicationProtocol.Version,
                "failure",
                AutomationExitCodes.InvalidPlan,
                invocation.CommandName,
                null,
                "rejected",
                null,
                [],
                [new ApplicationError(error.Code, error.Message)],
                []);
            return new AutomationRunResult(response.ExitCode, response, error.Message);
        }
        catch (OperationCanceledException)
        {
            bool timedOut = timeout?.IsCancellationRequested == true && !cancellationToken.IsCancellationRequested;
            string code = timedOut ? "timeout" : "cancelled";
            string message = timedOut ? "The Automation operation timed out." : "The Automation operation was cancelled.";
            AutomationProtocolResponse response = new(
                ApplicationProtocol.Version,
                "failure",
                AutomationExitCodes.CancelledOrTimedOut,
                invocation.CommandName,
                null,
                timedOut ? "timedOut" : "cancelled",
                null,
                [],
                [new ApplicationError(code, message)],
                []);
            return new AutomationRunResult(response.ExitCode, response, message);
        }
        catch (Exception error)
        {
            AutomationProtocolResponse response = AutomationProtocolResponse.InternalFailure(invocation.CommandName);
            return new AutomationRunResult(
                response.ExitCode,
                response,
                $"Automation internal failure ({error.GetType().Name}).");
        }
    }

    private static async Task<AutomationRunResult> DispatchAsync(
        IApplicationService service,
        AutomationInvocation invocation,
        CancellationTokenSource? timeout,
        CancellationToken callerToken,
        CancellationToken operationToken)
    {
        return invocation.Command switch
        {
            AutomationCommand.Describe => FromOutcome(
                invocation,
                await service.DescribeAsync(operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Status => FromOutcome(
                invocation,
                await service.GetStatusAsync(invocation.StatusRequest!, operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Plan => FromOutcome(
                invocation,
                await service.CreatePlanAsync(
                    new CreateApplicationPlanRequest(invocation.Intent!),
                    operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Sync => FromOutcome(
                invocation,
                await service.SyncAsync(
                    new SyncApplicationRequest(
                        (SyncIntent)invocation.Intent!,
                        await CreateAuthorizationAsync(invocation, operationToken)),
                    operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Switch => FromOutcome(
                invocation,
                await service.SwitchAsync(
                    new SwitchApplicationRequest(
                        (SwitchIntent)invocation.Intent!,
                        await CreateAuthorizationAsync(invocation, operationToken)),
                    operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Restore => FromOutcome(
                invocation,
                await service.RestoreAsync(
                    new RestoreApplicationRequest(
                        (RestoreIntent)invocation.Intent!,
                        await CreateAuthorizationAsync(invocation, operationToken)),
                    operationToken),
                DidTimeOut(timeout, callerToken)),
            AutomationCommand.Prune => FromOutcome(
                invocation,
                await service.PruneAsync(
                    new PruneApplicationRequest(
                        (PruneIntent)invocation.Intent!,
                        await CreateAuthorizationAsync(invocation, operationToken)),
                    operationToken),
                DidTimeOut(timeout, callerToken)),
            _ => throw new ArgumentOutOfRangeException(nameof(invocation.Command))
        };
    }

    private static bool DidTimeOut(CancellationTokenSource? timeout, CancellationToken callerToken)
    {
        return timeout?.IsCancellationRequested == true && !callerToken.IsCancellationRequested;
    }

    private static async Task<ApplicationApplyAuthorization> CreateAuthorizationAsync(
        AutomationInvocation invocation,
        CancellationToken cancellationToken)
    {
        if (!invocation.Apply)
        {
            return ApplicationApplyAuthorization.DryRun;
        }

        ApplicationOperationPlan plan = await ReadPlanAsync(invocation.PlanPath!, cancellationToken);
        return new ApplicationApplyAuthorization(
            Apply: true,
            Plan: plan,
            PlanDigest: invocation.PlanDigest);
    }

    private static async Task<ApplicationOperationPlan> ReadPlanAsync(
        string path,
        CancellationToken cancellationToken)
    {
        FileInfo info = new(path);
        if (!info.Exists || info.Length is <= 0 or > AutomationJson.MaximumPlanBytes)
        {
            throw new PlanDocumentException(
                "plan_document_invalid",
                $"The plan document must be between 1 and {AutomationJson.MaximumPlanBytes} bytes.");
        }

        byte[] bytes;
        try
        {
            await using FileStream stream = new(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            if (stream.Length is <= 0 or > AutomationJson.MaximumPlanBytes)
            {
                throw new PlanDocumentException(
                    "plan_document_invalid",
                    $"The plan document must be between 1 and {AutomationJson.MaximumPlanBytes} bytes.");
            }
            bytes = new byte[stream.Length];
            await stream.ReadExactlyAsync(bytes, cancellationToken);
        }
        catch (PlanDocumentException)
        {
            throw;
        }
        catch (IOException error)
        {
            throw new PlanDocumentException(
                "plan_document_unreadable",
                "The plan document could not be read safely.",
                error);
        }

        try
        {
            ApplicationOperationPlan? plan = JsonSerializer.Deserialize<ApplicationOperationPlan>(
                bytes,
                AutomationJson.Options);
            return plan ?? throw new PlanDocumentException(
                "plan_document_malformed",
                "The plan document is empty or malformed.");
        }
        catch (PlanDocumentException)
        {
            throw;
        }
        catch (Exception error) when (error is JsonException or NotSupportedException)
        {
            throw new PlanDocumentException(
                "plan_document_malformed",
                "The plan document is not valid protocol 0.4 JSON.",
                error);
        }
    }

    private static AutomationRunResult FromOutcome<T>(
        AutomationInvocation invocation,
        ApplicationOutcome<T> outcome,
        bool timedOut)
        where T : class
    {
        int exitCode = ExitCodeFor(outcome, timedOut);
        string result = ResultKindFor(outcome, exitCode);
        IReadOnlyList<ApplicationError> errors = timedOut
            && outcome.Lifecycle == ApplicationOperationLifecycle.Cancelled
            ? [new ApplicationError("timeout", "The Automation operation timed out.")]
            : outcome.Errors;
        string lifecycle = timedOut && outcome.Lifecycle == ApplicationOperationLifecycle.Cancelled
            ? "timedOut"
            : ToProtocolName(outcome.Lifecycle);
        AutomationProtocolResponse response = new(
            ApplicationProtocol.Version,
            result,
            exitCode,
            invocation.CommandName,
            outcome.OperationId,
            lifecycle,
            outcome.Data,
            outcome.Warnings,
            errors,
            outcome.Timeline);
        string? diagnostic = errors.Count == 0
            ? null
            : string.Join("; ", errors.Select(static error => $"{error.Code}: {error.Message}"));
        return new AutomationRunResult(exitCode, response, diagnostic);
    }

    private static int ExitCodeFor<T>(ApplicationOutcome<T> outcome, bool timedOut)
        where T : class
    {
        if (timedOut && outcome.Lifecycle == ApplicationOperationLifecycle.Cancelled)
        {
            return AutomationExitCodes.CancelledOrTimedOut;
        }
        if (outcome.Errors.Any(static error => error.Code == "capability_unavailable"))
        {
            return AutomationExitCodes.ValidationOrUsage;
        }
        if (outcome.Errors.Any(static error => error.RecoveryRequired))
        {
            return AutomationExitCodes.RecoveryRequired;
        }
        if (outcome.Errors.Any(static error => error.Code is
            "invalid_normalized_intent" or
            "invalid_plan_preview" or
            "invalid_plan_targets" or
            "duplicate_plan_target" or
            "invalid_auto_prune_targets" or
            "duplicate_auto_prune_target" or
            "invalid_plan_warning"))
        {
            return AutomationExitCodes.InternalProtocolFailure;
        }

        return outcome.Lifecycle switch
        {
            ApplicationOperationLifecycle.Succeeded or ApplicationOperationLifecycle.ReadyToApply =>
                AutomationExitCodes.Success,
            ApplicationOperationLifecycle.Cancelled => AutomationExitCodes.CancelledOrTimedOut,
            ApplicationOperationLifecycle.RecoveryRequired => AutomationExitCodes.RecoveryRequired,
            ApplicationOperationLifecycle.Failed => ExitCodeForFailedOutcome(outcome),
            ApplicationOperationLifecycle.Rejected when outcome.Errors.Any(static error =>
                error.Code is "operation_busy" or "target_busy" or "lock_unverifiable") => AutomationExitCodes.Busy,
            ApplicationOperationLifecycle.Rejected when outcome.Errors.Any(static error =>
                error.Code.StartsWith("plan_", StringComparison.Ordinal)) => AutomationExitCodes.InvalidPlan,
            ApplicationOperationLifecycle.Rejected => AutomationExitCodes.ValidationOrUsage,
            _ => AutomationExitCodes.InternalProtocolFailure
        };
    }

    private static int ExitCodeForFailedOutcome<T>(ApplicationOutcome<T> outcome)
        where T : class
    {
        if (outcome.Errors.Any(static error => string.Equals(
                error.RollbackStatus,
                "complete",
                StringComparison.Ordinal)))
        {
            return AutomationExitCodes.RolledBackFailure;
        }

        // A failure before the mutation boundary is a validation/usage failure.
        // Once Applying has started, unknown rollback state must fail closed.
        return outcome.Timeline.Any(static entry =>
            entry.Lifecycle == ApplicationOperationLifecycle.Applying)
            ? AutomationExitCodes.InternalProtocolFailure
            : AutomationExitCodes.ValidationOrUsage;
    }

    private static string ResultKindFor<T>(ApplicationOutcome<T> outcome, int exitCode)
        where T : class
    {
        if (exitCode == AutomationExitCodes.RecoveryRequired)
        {
            return "recovery";
        }
        if (outcome.Errors.Any(static error => string.Equals(error.RollbackStatus, "complete", StringComparison.Ordinal)))
        {
            return "rollback";
        }
        if (exitCode != AutomationExitCodes.Success)
        {
            return "failure";
        }
        return outcome.Warnings.Count > 0 ? "warning" : "success";
    }

    private static string ToProtocolName(ApplicationOperationLifecycle lifecycle)
    {
        string name = lifecycle.ToString();
        return char.ToLowerInvariant(name[0]) + name[1..];
    }

    private sealed class PlanDocumentException : IOException
    {
        public PlanDocumentException(string code, string message, Exception? innerException = null)
            : base(message, innerException)
        {
            Code = code;
        }

        public string Code { get; }
    }
}
