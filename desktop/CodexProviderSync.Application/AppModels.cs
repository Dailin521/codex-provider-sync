using CodexProviderSync.Core;
using System.Text.Json.Serialization;

namespace CodexProviderSync.Application;

public enum AppActivity
{
    Uninitialized,
    Initializing,
    Refreshing,
    Ready,
    Faulted
}

public enum ModelMode
{
    FollowProvider,
    KeepRootModel,
    Custom
}

public enum AppValidationIssue
{
    OperationInProgress,
    RefreshFailed,
    StatusUnavailable,
    ProviderRequired,
    CustomModelRequired,
    SqliteUnsupported
}

public sealed record ProviderOptionState(
    string Id,
    IReadOnlyList<ProviderSource> Sources,
    bool IsCurrentProvider,
    bool IsManual,
    bool IsSaved,
    bool IsSelected);

public sealed record AppControlAvailability(
    bool RefreshEnabled,
    bool ExecuteEnabled,
    bool ProviderSelectionEnabled,
    bool UpdateConfigEnabled,
    bool ModelModeEnabled,
    bool CustomModelEnabled);

public sealed record AppSnapshot
{
    internal AppSnapshot()
    {
    }

    public AppActivity Activity { get; internal init; } = AppActivity.Uninitialized;
    public string CodexHome { get; internal init; } = string.Empty;
    public string? SqliteHomeOverride { get; internal init; }
    public StatusSnapshot? Status { get; internal init; }
    public IReadOnlyList<ProviderOptionState> Providers { get; internal init; } = [];
    public string? SelectedProviderId { get; internal init; }
    public bool UpdateConfig { get; internal init; }
    public ModelMode ModelMode { get; internal init; } = ModelMode.FollowProvider;
    public string CustomModel { get; internal init; } = string.Empty;
    public IReadOnlyList<AppValidationIssue> ValidationIssues { get; internal init; } = [];
    public AppControlAvailability Controls { get; internal init; } = new(
        RefreshEnabled: false,
        ExecuteEnabled: false,
        ProviderSelectionEnabled: false,
        UpdateConfigEnabled: false,
        ModelModeEnabled: false,
        CustomModelEnabled: false);
    public string? ErrorMessage { get; internal init; }

    public ProviderOptionState? SelectedProvider => Providers.FirstOrDefault(static option => option.IsSelected);

    public bool HasIssue(AppValidationIssue issue) => ValidationIssues.Contains(issue);
}

public abstract record PreparedSyncRequest(
    string CodexHome,
    string? SqliteHomeOverride,
    string ProviderId);

public sealed record SyncProviderRequest(
    string CodexHome,
    string? SqliteHomeOverride,
    string ProviderId)
    : PreparedSyncRequest(CodexHome, SqliteHomeOverride, ProviderId);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "mode")]
[JsonDerivedType(typeof(FollowProviderModelSelection), "followProvider")]
[JsonDerivedType(typeof(KeepRootModelSelection), "keepRootModel")]
[JsonDerivedType(typeof(CustomModelSelection), "custom")]
public abstract record SwitchModelSelection;

public sealed record FollowProviderModelSelection : SwitchModelSelection;

public sealed record KeepRootModelSelection : SwitchModelSelection;

public sealed record CustomModelSelection(string Model) : SwitchModelSelection;

public sealed record SwitchProviderRequest(
    string CodexHome,
    string? SqliteHomeOverride,
    string ProviderId,
    SwitchModelSelection ModelSelection)
    : PreparedSyncRequest(CodexHome, SqliteHomeOverride, ProviderId);

public sealed record SyncRequestPreparation(
    PreparedSyncRequest? Request,
    IReadOnlyList<AppValidationIssue> ValidationIssues)
{
    public bool IsValid => Request is not null && ValidationIssues.Count == 0;
}
