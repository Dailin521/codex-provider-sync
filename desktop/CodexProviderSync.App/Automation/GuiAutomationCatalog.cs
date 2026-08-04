using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace CodexProviderSync.App.Automation;

/// <summary>
/// Stable, pre-1.0 identifiers for the real WinForms surface. These identifiers
/// are presentation metadata only; they do not expose an automation listener.
/// </summary>
internal static class GuiAutomationCatalog
{
    internal const string ManifestResourceName =
        "CodexProviderSync.App.Automation.gui-automation-manifest.v0.4.json";

    internal static class Ids
    {
        internal const string MainWindow = "window.main";
        internal const string CodexHome = "storage.codexHome";
        internal const string BrowseCodexHome = "storage.codexHome.browse";
        internal const string SqliteHome = "storage.sqliteHome";
        internal const string BrowseSqliteHome = "storage.sqliteHome.browse";
        internal const string RefreshStatus = "status.refresh";
        internal const string StatusOutput = "status.output";
        internal const string ProviderList = "provider.list";
        internal const string ManualProviderId = "provider.manualId";
        internal const string AddManualProvider = "provider.addManual";
        internal const string RemoveManualProvider = "provider.removeManual";
        internal const string SelectedProvider = "state.selectedProvider";
        internal const string UpdateConfig = "execution.updateConfig";
        internal const string FollowProviderModel = "execution.model.followProvider";
        internal const string KeepCurrentModel = "execution.model.keepCurrent";
        internal const string CustomModelMode = "execution.model.custom";
        internal const string CustomModel = "execution.customModel";
        internal const string RestoreConfig = "restore.includeConfig";
        internal const string RestoreDatabase = "restore.includeDatabase";
        internal const string RestoreSessions = "restore.includeSessions";
        internal const string BackupRetention = "backup.retentionCount";
        internal const string Execute = "operation.execute";
        internal const string Restore = "restore.execute";
        internal const string OpenBackupDirectory = "backups.openDirectory";
        internal const string PruneBackups = "backups.prune";
        internal const string CheckUpdates = "updates.check";
        internal const string OpenLogDirectory = "logs.openDirectory";
        internal const string OperationState = "state.operation";
        internal const string CloseCodexWarningPrimary = "warning.closeCodex.primary";
        internal const string CloseCodexWarningSecondary = "warning.closeCodex.secondary";
        internal const string LogOutput = "logs.output";
    }

    internal static class Templates
    {
        internal const string ProviderRow = "provider.row";
        internal const string RecentCodexHome = "storage.codexHome.recent";
    }

    internal static T Register<T>(T control, string automationId) where T : Control
    {
        ArgumentNullException.ThrowIfNull(control);
        ValidateIdentifier(automationId);
        control.Name = automationId;
        control.AccessibleName = string.IsNullOrWhiteSpace(control.AccessibleName)
            ? string.IsNullOrWhiteSpace(control.Text) ? automationId : control.Text
            : control.AccessibleName;
        control.AccessibleDescription = $"AutomationId:{automationId}";
        return control;
    }

    internal static void RegisterProviderRow(ListViewItem item, string providerId)
    {
        ArgumentNullException.ThrowIfNull(item);
        item.Name = InstanceId(Templates.ProviderRow, providerId);
    }

    internal static AutomationComboBoxItem RecentCodexHome(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        string normalizedPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        if (OperatingSystem.IsWindows())
        {
            normalizedPath = normalizedPath.ToUpperInvariant();
        }
        return new AutomationComboBoxItem(
            path,
            Templates.RecentCodexHome,
            InstanceKey(normalizedPath));
    }

    internal static IEnumerable<Control> EnumerateRuntimeDenominator(MainForm form)
    {
        yield return form;
        foreach (Control control in Descendants(form))
        {
            if (HasRegisteredCompositeAncestor(control))
            {
                continue;
            }
            if (control is ButtonBase or TextBoxBase or ComboBox or ListView or NumericUpDown
                || IsMeaningfulStateSurface(control))
            {
                yield return control;
            }
        }
    }

    internal static void ValidateRuntimeCoverage(MainForm form)
    {
        Control[] runtime = EnumerateRuntimeDenominator(form).ToArray();
        if (runtime.Any(control => string.IsNullOrWhiteSpace(control.Name)
            || string.IsNullOrWhiteSpace(control.AccessibleName)
            || !string.Equals(
                control.AccessibleDescription,
                $"AutomationId:{control.Name}",
                StringComparison.Ordinal)))
        {
            throw new InvalidOperationException("The GUI runtime denominator contains an unregistered automation surface.");
        }
        string[] duplicates = runtime
            .GroupBy(control => control.Name, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray();
        if (duplicates.Length > 0)
        {
            throw new InvalidOperationException($"Duplicate GUI automation ids: {string.Join(", ", duplicates)}");
        }

        using Stream stream = typeof(MainForm).Assembly.GetManifestResourceStream(ManifestResourceName)
            ?? throw new InvalidOperationException("Embedded GUI automation manifest was not found.");
        using JsonDocument manifest = JsonDocument.Parse(stream);
        Dictionary<string, JsonElement> declaredControls = manifest.RootElement.GetProperty("controls")
            .EnumerateArray()
            .ToDictionary(
                item => item.GetProperty("id").GetString()!,
                item => item,
                StringComparer.Ordinal);
        JsonElement window = manifest.RootElement.GetProperty("window");
        HashSet<string> declared = declaredControls.Keys.ToHashSet(StringComparer.Ordinal);
        declared.Add(window.GetProperty("id").GetString()!);
        HashSet<string> actual = runtime.Select(control => control.Name).ToHashSet(StringComparer.Ordinal);
        if (!declared.SetEquals(actual))
        {
            string missing = string.Join(", ", declared.Except(actual, StringComparer.Ordinal));
            string undeclared = string.Join(", ", actual.Except(declared, StringComparer.Ordinal));
            throw new InvalidOperationException(
                $"GUI automation manifest/runtime mismatch. Missing: [{missing}]. Undeclared: [{undeclared}].");
        }

        foreach (Control control in runtime)
        {
            JsonElement declaration = control == form
                ? window
                : declaredControls[control.Name];
            string declaredType = declaration.GetProperty("controlType").GetString()!;
            string runtimeType = control == form ? nameof(Form) : control.GetType().Name;
            if (!string.Equals(declaredType, runtimeType, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"GUI automation manifest type mismatch for {control.Name}: {declaredType} != {runtimeType}.");
            }
            foreach (JsonElement action in declaration.GetProperty("actions").EnumerateArray())
            {
                string name = action.GetString()!;
                if (!GuiAutomationActionCoverage.SupportsControl(control, name))
                {
                    throw new InvalidOperationException(
                        $"GUI automation action is declared but not implemented: {control.Name}.{name}.");
                }
            }
        }

        Dictionary<string, JsonElement> templates = manifest.RootElement.GetProperty("templates")
            .EnumerateArray()
            .ToDictionary(
                item => item.GetProperty("id").GetString()!,
                item => item,
                StringComparer.Ordinal);
        string[] expectedTemplates = [Templates.ProviderRow, Templates.RecentCodexHome];
        if (!templates.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(expectedTemplates))
        {
            throw new InvalidOperationException("GUI automation dynamic-template coverage is incomplete.");
        }
        foreach ((string templateId, JsonElement declaration) in templates)
        {
            foreach (JsonElement action in declaration.GetProperty("actions").EnumerateArray())
            {
                string name = action.GetString()!;
                if (!GuiAutomationActionCoverage.SupportsTemplate(templateId, name))
                {
                    throw new InvalidOperationException(
                        $"GUI automation template action is declared but not implemented: {templateId}.{name}.");
                }
            }
        }
    }

    private static IEnumerable<Control> Descendants(Control control)
    {
        foreach (Control child in control.Controls)
        {
            yield return child;
            foreach (Control descendant in Descendants(child))
            {
                yield return descendant;
            }
        }
    }

    private static bool HasRegisteredCompositeAncestor(Control control)
    {
        for (Control? ancestor = control.Parent; ancestor is not null; ancestor = ancestor.Parent)
        {
            if (ancestor is NumericUpDown)
            {
                return true;
            }
        }
        return false;
    }

    private static bool IsMeaningfulStateSurface(Control control)
    {
        return control is Label label
            && (label.Font.Bold
                || label.ForeColor is var foreground && (foreground == Color.DarkGreen || foreground == Color.DarkOrange)
                || label.BackColor == Color.FromArgb(255, 244, 214));
    }

    internal static string InstanceId(string templateId, string naturalKey)
    {
        ValidateIdentifier(templateId);
        return $"{templateId}:{InstanceKey(naturalKey)}";
    }

    internal static string InstanceKey(string naturalKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(naturalKey);
        string normalized = naturalKey.Trim().Normalize(NormalizationForm.FormKC);
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(normalized));
        return Convert.ToHexString(digest.AsSpan(0, 12)).ToLowerInvariant();
    }

    private static void ValidateIdentifier(string automationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(automationId);
        if (automationId.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '.' or '-')))
        {
            throw new ArgumentException(
                $"Automation identifier contains an unsupported character: {automationId}",
                nameof(automationId));
        }
    }
}

internal static class GuiAutomationActionCoverage
{
    internal static bool SupportsControl(Control control, string action)
    {
        ArgumentNullException.ThrowIfNull(control);
        return control switch
        {
            MainForm => action is "get" or "focus" or "close",
            Button => action == "invoke",
            CheckBox => action is "get" or "set" or "toggle",
            RadioButton => action is "get" or "set",
            ComboBox when control.Name == GuiAutomationCatalog.Ids.CodexHome =>
                action is "get" or "set" or "select",
            ComboBox => action is "get" or "set",
            ListView when control.Name == GuiAutomationCatalog.Ids.ProviderList =>
                action is "get" or "select",
            NumericUpDown => action is "get" or "set",
            TextBoxBase textBox when textBox.ReadOnly => action == "get",
            TextBoxBase => action is "get" or "set",
            Label => action == "get",
            _ => false
        };
    }

    internal static bool SupportsTemplate(string templateId, string action)
    {
        return templateId switch
        {
            GuiAutomationCatalog.Templates.ProviderRow => action is "get" or "select",
            GuiAutomationCatalog.Templates.RecentCodexHome => action is "get" or "select",
            _ => false
        };
    }
}

internal sealed record AutomationComboBoxItem(
    string Value,
    string TemplateId,
    string InstanceKey)
{
    internal string AutomationId => $"{TemplateId}:{InstanceKey}";

    public override string ToString() => Value;
}
