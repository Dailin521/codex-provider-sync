using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CodexProviderSync.Core;

public sealed partial class ConfigFileService
{
    [GeneratedRegex("""^\[model_providers\.([A-Za-z0-9_.-]+)]\s*$""", RegexOptions.Multiline)]
    private static partial Regex ProviderRegex();

    public Task<string> ReadConfigTextAsync(string configPath)
    {
        return File.ReadAllTextAsync(configPath);
    }

    public async Task WriteConfigTextAsync(string configPath, string configText)
    {
        await File.WriteAllTextAsync(configPath, configText);
    }

    public CurrentProviderInfo ReadCurrentProviderFromConfigText(string configText)
    {
        foreach (string rawLine in SplitLines(configText))
        {
            string trimmed = rawLine.Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || trimmed.StartsWith('#'))
            {
                continue;
            }

            if (trimmed.StartsWith('['))
            {
                break;
            }

            Match match = Regex.Match(trimmed, "^model_provider\\s*=\\s*\"([^\"]+)\"\\s*$");
            if (match.Success)
            {
                return new CurrentProviderInfo(match.Groups[1].Value, false);
            }
        }

        return new CurrentProviderInfo(AppConstants.DefaultProvider, true);
    }

    public IReadOnlyList<string> ListConfiguredProviderIds(string configText)
    {
        HashSet<string> providerIds = new(StringComparer.Ordinal)
        {
            AppConstants.DefaultProvider
        };

        foreach (Match match in ProviderRegex().Matches(configText))
        {
            providerIds.Add(match.Groups[1].Value);
        }

        return providerIds.OrderBy(static value => value, StringComparer.Ordinal).ToList();
    }

    public bool ConfigDeclaresProvider(string configText, string provider)
    {
        return ListConfiguredProviderIds(configText).Contains(provider, StringComparer.Ordinal);
    }

    public string SetRootProviderInConfigText(string configText, string provider)
    {
        string newline = DetectNewline(configText);
        List<string> lines = SplitLines(configText).ToList();
        int insertIndex = lines.Count;

        for (int index = 0; index < lines.Count; index += 1)
        {
            string trimmed = lines[index].Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || trimmed.StartsWith('#'))
            {
                insertIndex = index + 1;
                continue;
            }

            if (trimmed.StartsWith('['))
            {
                insertIndex = index;
                break;
            }

            if (trimmed.StartsWith("model_provider =", StringComparison.Ordinal))
            {
                lines[index] = $"model_provider = \"{EscapeTomlString(provider)}\"";
                return string.Join(newline, lines) + (configText.EndsWith(newline, StringComparison.Ordinal) ? newline : string.Empty);
            }

            insertIndex = index + 1;
        }

        lines.Insert(insertIndex, $"model_provider = \"{EscapeTomlString(provider)}\"");
        string nextText = string.Join(newline, lines);
        return configText.EndsWith(newline, StringComparison.Ordinal) ? nextText + newline : nextText;
    }

    private static IEnumerable<string> SplitLines(string text)
    {
        return text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
    }

    private static string DetectNewline(string text)
    {
        return text.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
    }

    private static string EscapeTomlString(string value)
    {
        return value.Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "\\\"", StringComparison.Ordinal);
    }
}
