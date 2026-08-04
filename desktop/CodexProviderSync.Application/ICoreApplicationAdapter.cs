using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public sealed record CoreInitializationState(
    string CodexHome,
    string? SqliteHomeOverride,
    string? PreferredProviderId);

public sealed record CoreRefreshRequest(
    string CodexHome,
    string? SqliteHomeOverride,
    string? SelectedProviderId);

public sealed record CoreRefreshState(
    StatusSnapshot Status,
    IReadOnlyList<ProviderOption> Providers);

public interface ICoreApplicationAdapter
{
    Task<CoreInitializationState> InitializeAsync(CancellationToken cancellationToken = default);

    Task<CoreRefreshState> RefreshAsync(
        CoreRefreshRequest request,
        CancellationToken cancellationToken = default);
}
