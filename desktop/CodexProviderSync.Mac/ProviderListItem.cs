using CodexProviderSync.Core;

namespace CodexProviderSync.Mac;

internal sealed class ProviderListItem
{
    public required ProviderOption Option { get; init; }
    public required string SourcesText { get; init; }
    public required string DetailText { get; init; }
}
