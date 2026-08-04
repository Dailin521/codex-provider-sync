using System.Text.Json;
using System.Text.Json.Serialization;
using CodexProviderSync.Application;

namespace CodexProviderSync.Automation;

public static class AutomationExitCodes
{
    public const int Success = 0;
    public const int ValidationOrUsage = 2;
    public const int InvalidPlan = 3;
    public const int Busy = 4;
    public const int RolledBackFailure = 5;
    public const int RecoveryRequired = 6;
    public const int CancelledOrTimedOut = 7;
    public const int InternalProtocolFailure = 10;
}

public static class AutomationJson
{
    public const int MaximumPlanBytes = 1024 * 1024;

    public static JsonSerializerOptions Options { get; } = CreateOptions();

    private static JsonSerializerOptions CreateOptions()
    {
        JsonSerializerOptions options = new(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            AllowTrailingCommas = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            WriteIndented = false
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }
}

public sealed record AutomationProtocolResponse(
    string ProtocolVersion,
    string Result,
    int ExitCode,
    string Command,
    string? OperationId,
    string Lifecycle,
    object? Data,
    IReadOnlyList<ApplicationWarning> Warnings,
    IReadOnlyList<ApplicationError> Errors,
    IReadOnlyList<ApplicationLifecycleEvent> Timeline)
{
    public static AutomationProtocolResponse UsageFailure(
        string command,
        string code,
        string message)
    {
        return new AutomationProtocolResponse(
            ApplicationProtocol.Version,
            "failure",
            AutomationExitCodes.ValidationOrUsage,
            command,
            null,
            "rejected",
            null,
            [],
            [new ApplicationError(code, message)],
            []);
    }

    public static AutomationProtocolResponse InternalFailure(string command)
    {
        return new AutomationProtocolResponse(
            ApplicationProtocol.Version,
            "failure",
            AutomationExitCodes.InternalProtocolFailure,
            command,
            null,
            "failed",
            null,
            [],
            [new ApplicationError(
                "internal_protocol_failure",
                "The Automation host could not complete the protocol operation.")],
            []);
    }
}

public sealed record AutomationRunResult(
    int ExitCode,
    AutomationProtocolResponse Response,
    string? Diagnostic = null);
