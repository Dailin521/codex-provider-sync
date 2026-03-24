using System.Collections.Generic;
using System.Linq;

namespace CodexProviderSync.Core;

public sealed class ProviderDiscoveryService
{
    public IReadOnlyList<ProviderOption> BuildProviderOptions(StatusSnapshot status, AppSettings settings)
    {
        Dictionary<string, HashSet<ProviderSource>> sources = new(StringComparer.Ordinal);

        void AddSources(IEnumerable<string> providerIds, ProviderSource source)
        {
            foreach (string providerId in providerIds.Where(static value => !string.IsNullOrWhiteSpace(value)))
            {
                if (!sources.TryGetValue(providerId, out HashSet<ProviderSource>? bucket))
                {
                    bucket = [];
                    sources[providerId] = bucket;
                }

                bucket.Add(source);
            }
        }

        AddSources(status.ConfiguredProviders, ProviderSource.Config);
        AddSources(status.RolloutCounts.Sessions.Keys, ProviderSource.Rollout);
        AddSources(status.RolloutCounts.ArchivedSessions.Keys, ProviderSource.Rollout);
        if (status.SqliteCounts is not null)
        {
            AddSources(status.SqliteCounts.Sessions.Keys, ProviderSource.Sqlite);
            AddSources(status.SqliteCounts.ArchivedSessions.Keys, ProviderSource.Sqlite);
        }

        AddSources(settings.SavedProviders, ProviderSource.Manual);
        AddSources(settings.ManualProviders, ProviderSource.Manual);
        AddSources([status.CurrentProvider.Provider], ProviderSource.Config);

        HashSet<string> manualProviders = new(settings.ManualProviders, StringComparer.Ordinal);
        HashSet<string> savedProviders = new(settings.SavedProviders, StringComparer.Ordinal);

        return sources
            .OrderByDescending(pair => string.Equals(pair.Key, status.CurrentProvider.Provider, StringComparison.Ordinal))
            .ThenBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => new ProviderOption
            {
                Id = pair.Key,
                Sources = pair.Value.Order().ToList(),
                IsCurrentProvider = string.Equals(pair.Key, status.CurrentProvider.Provider, StringComparison.Ordinal),
                IsManual = manualProviders.Contains(pair.Key),
                IsSaved = savedProviders.Contains(pair.Key)
            })
            .ToList();
    }

    public IReadOnlyList<string> ExtractDetectedProviderIds(StatusSnapshot status)
    {
        HashSet<string> providers = new(StringComparer.Ordinal);
        foreach (string provider in status.ConfiguredProviders)
        {
            providers.Add(provider);
        }

        foreach (string provider in status.RolloutCounts.Sessions.Keys)
        {
            providers.Add(provider);
        }

        foreach (string provider in status.RolloutCounts.ArchivedSessions.Keys)
        {
            providers.Add(provider);
        }

        if (status.SqliteCounts is not null)
        {
            foreach (string provider in status.SqliteCounts.Sessions.Keys)
            {
                providers.Add(provider);
            }

            foreach (string provider in status.SqliteCounts.ArchivedSessions.Keys)
            {
                providers.Add(provider);
            }
        }

        providers.Add(status.CurrentProvider.Provider);
        return providers.Order(StringComparer.Ordinal).ToList();
    }
}
