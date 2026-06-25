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

    public string SetRootModelInConfigText(string configText, string model)
    {
        if (string.IsNullOrEmpty(model))
        {
            throw new ArgumentException("Model must be a non-empty string.", nameof(model));
        }

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

            if (trimmed.StartsWith("model =", StringComparison.Ordinal))
            {
                lines[index] = $"model = \"{EscapeTomlString(model)}\"";
                return string.Join(newline, lines) + (configText.EndsWith(newline, StringComparison.Ordinal) ? newline : string.Empty);
            }

            insertIndex = index + 1;
        }

        lines.Insert(insertIndex, $"model = \"{EscapeTomlString(model)}\"");
        string nextText = string.Join(newline, lines);
        return configText.EndsWith(newline, StringComparison.Ordinal) ? nextText + newline : nextText;
    }

    public string? ReadProviderModel(string configText, string provider)
    {
        if (string.Equals(provider, AppConstants.DefaultProvider, StringComparison.Ordinal))
        {
            // Built-in openai has no [model_providers.openai] section.
            return null;
        }

        string? sectionStart = null;
        int sectionStartIndex = -1;
        for (int index = 0; index < SplitLines(configText).Count(); index += 1)
        {
            string trimmed = SplitLines(configText).ElementAt(index).Trim();
            if (trimmed.Equals($"[model_providers.{provider}]", StringComparison.Ordinal))
            {
                sectionStart = $"[model_providers.{provider}]";
                sectionStartIndex = index;
                break;
            }
        }

        if (sectionStart is null)
        {
            return null;
        }

        List<string> lines = SplitLines(configText).ToList();
        for (int index = sectionStartIndex + 1; index < lines.Count; index += 1)
        {
            string trimmed = lines[index].Trim();
            if (trimmed.StartsWith('['))
            {
                break;
            }
            Match match = Regex.Match(lines[index], "^\\s*model\\s*=\\s*\"([^\"]+)\"\\s*$");
            if (match.Success)
            {
                return match.Groups[1].Value;
            }
        }

        return null;
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
