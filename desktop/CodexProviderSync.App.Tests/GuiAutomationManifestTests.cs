using System.Drawing;
using System.Reflection;
using System.Text.Json;
using System.Windows.Forms;
using CodexProviderSync.App.Automation;
using CodexProviderSync.Core;

namespace CodexProviderSync.App.Tests;

/// <summary>
/// Contract tests for the static manifest and the real MainForm control tree.
/// These are in-process WinForms tests, not headful GUI E2E evidence.
/// </summary>
public sealed class GuiAutomationManifestTests
{
    [Fact]
    public void RealMainForm_RuntimeDenominatorMatchesManifestWithoutMissingOrDuplicateIds()
    {
        using JsonDocument manifest = LoadManifest();
        using IsolatedMainForm fixture = new();

        JsonElement root = manifest.RootElement;
        JsonElement window = root.GetProperty("window");
        JsonElement[] declaredControls = root.GetProperty("controls").EnumerateArray().ToArray();
        Dictionary<string, JsonElement> declaredById = declaredControls.ToDictionary(
            declaration => declaration.GetProperty("id").GetString()!,
            StringComparer.Ordinal);

        Control[] runtimeDenominator = EnumerateRuntimeDenominator(fixture.Form).ToArray();
        Assert.Equal(31, runtimeDenominator.Length);
        Assert.All(runtimeDenominator, control =>
        {
            Assert.False(string.IsNullOrWhiteSpace(control.Name));
            Assert.False(string.IsNullOrWhiteSpace(control.AccessibleName));
            Assert.Equal($"AutomationId:{control.Name}", control.AccessibleDescription);
        });
        Button browse = fixture.Form.Controls.Find(GuiAutomationCatalog.Ids.BrowseCodexHome, true)
            .OfType<Button>()
            .Single();
        Assert.Equal(browse.Text, browse.AccessibleName);
        Assert.NotEqual(browse.Name, browse.AccessibleName);

        string[] runtimeIds = runtimeDenominator.Select(control => control.Name).ToArray();
        Assert.DoesNotContain(
            runtimeIds.GroupBy(id => id, StringComparer.Ordinal),
            group => group.Count() > 1);

        HashSet<string> declaredIds = declaredById.Keys.ToHashSet(StringComparer.Ordinal);
        declaredIds.Add(window.GetProperty("id").GetString()!);
        Assert.Equal(declaredIds.Order(StringComparer.Ordinal), runtimeIds.Order(StringComparer.Ordinal));

        Assert.Equal("0.4", root.GetProperty("schemaVersion").GetString());
        Assert.Equal(26, declaredControls.Count(control => control.GetProperty("logicalControl").GetBoolean()));
        Assert.Equal(GuiAutomationCatalog.Ids.MainWindow, window.GetProperty("id").GetString());
        Assert.Equal("Form", window.GetProperty("controlType").GetString());
        Assert.All(window.GetProperty("actions").EnumerateArray(), action =>
            Assert.True(
                GuiAutomationActionCoverage.SupportsControl(fixture.Form, action.GetString()!),
                $"Missing window action implementation: {action.GetString()}"));

        foreach (Control control in runtimeDenominator.Where(control => control != fixture.Form))
        {
            JsonElement declaration = declaredById[control.Name];
            Assert.Equal(control.GetType().Name, declaration.GetProperty("controlType").GetString());
            Assert.NotEmpty(declaration.GetProperty("actions").EnumerateArray());
            Assert.False(string.IsNullOrWhiteSpace(declaration.GetProperty("visibleWhen").GetString()));
            Assert.False(string.IsNullOrWhiteSpace(declaration.GetProperty("enabledWhen").GetString()));
            Assert.Equal(JsonValueKind.Array, declaration.GetProperty("applicationCapabilities").ValueKind);
            Assert.True(declaration.GetProperty("uiOnly").ValueKind is JsonValueKind.True or JsonValueKind.False);
            Assert.False(string.IsNullOrWhiteSpace(declaration.GetProperty("risk").GetString()));
            Assert.NotEmpty(declaration.GetProperty("scenarioIds").EnumerateArray());
            Assert.All(declaration.GetProperty("actions").EnumerateArray(), action =>
                Assert.True(
                    GuiAutomationActionCoverage.SupportsControl(control, action.GetString()!),
                    $"Missing action implementation: {control.Name}.{action.GetString()}"));
        }
    }

    [Fact]
    public void RealMainForm_StableIdsSurvivePresentationAndBusyStateChanges()
    {
        using IsolatedMainForm fixture = new();
        MainForm form = fixture.Form;
        Dictionary<Control, (string Name, string? AccessibleName)> before =
            EnumerateRuntimeDenominator(form).ToDictionary(
                control => control,
                control => (control.Name, control.AccessibleName));

        Field<CheckBox>(form, "_updateConfigCheck").Checked = true;
        Field<RadioButton>(form, "_modelCustomRadio").Checked = true;
        Field<TextBox>(form, "_modelCustomText").Text = "example-model";
        Field<ComboBox>(form, "_codexHomeCombo").Text = fixture.HomeA;
        Invoke(form, "SetBusy", true, "执行中...");
        Invoke(form, "SetBusy", false, "就绪");
        form.Size = form.MinimumSize;
        form.PerformLayout();

        Control[] after = EnumerateRuntimeDenominator(form).ToArray();
        Assert.Equal(before.Count, after.Length);
        Assert.All(after, control => Assert.Equal(before[control], (control.Name, control.AccessibleName)));
    }

    [Fact]
    public void DynamicEntries_UseDeclaredTemplatesAndOrderIndependentHashedInstanceKeys()
    {
        using JsonDocument manifest = LoadManifest();
        string[] templateIds = manifest.RootElement.GetProperty("templates")
            .EnumerateArray()
            .Select(template => template.GetProperty("id").GetString()!)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            new[] { GuiAutomationCatalog.Templates.ProviderRow, GuiAutomationCatalog.Templates.RecentCodexHome }
                .Order(StringComparer.Ordinal),
            templateIds);
        Assert.All(manifest.RootElement.GetProperty("templates").EnumerateArray(), template =>
        {
            string templateId = template.GetProperty("id").GetString()!;
            Assert.All(template.GetProperty("actions").EnumerateArray(), action =>
                Assert.True(
                    GuiAutomationActionCoverage.SupportsTemplate(templateId, action.GetString()!),
                    $"Missing template action implementation: {templateId}.{action.GetString()}"));
        });

        ListViewItem firstProviderRow = new("visible text one");
        ListViewItem secondProviderRow = new("different visible text");
        GuiAutomationCatalog.RegisterProviderRow(firstProviderRow, "  api-gather  ");
        GuiAutomationCatalog.RegisterProviderRow(secondProviderRow, "api-gather");
        Assert.Equal(firstProviderRow.Name, secondProviderRow.Name);
        Assert.StartsWith($"{GuiAutomationCatalog.Templates.ProviderRow}:", firstProviderRow.Name, StringComparison.Ordinal);
        Assert.DoesNotContain("api-gather", firstProviderRow.Name, StringComparison.OrdinalIgnoreCase);

        using IsolatedMainForm fixture = new();
        SetField(fixture.Form, "_settings", new AppSettings
        {
            RecentCodexHomes = [fixture.HomeA, fixture.HomeB]
        });
        Invoke(fixture.Form, "ReloadRecentHomes");
        ComboBox combo = Field<ComboBox>(fixture.Form, "_codexHomeCombo");
        AutomationComboBoxItem[] original = combo.Items.Cast<AutomationComboBoxItem>().ToArray();
        Assert.Equal([fixture.HomeA, fixture.HomeB], original.Select(item => item.Value));
        AutomationComboBoxItem equivalentRecentHome = GuiAutomationCatalog.RecentCodexHome(
            fixture.HomeA.ToUpperInvariant() + Path.DirectorySeparatorChar);
        Assert.Equal(original[0].InstanceKey, equivalentRecentHome.InstanceKey);
        Assert.All(original, item =>
        {
            Assert.Equal(GuiAutomationCatalog.Templates.RecentCodexHome, item.TemplateId);
            Assert.StartsWith($"{GuiAutomationCatalog.Templates.RecentCodexHome}:", item.AutomationId, StringComparison.Ordinal);
            Assert.DoesNotContain(item.Value, item.AutomationId, StringComparison.OrdinalIgnoreCase);
        });
        combo.SelectedIndex = 0;
        Assert.Equal(fixture.HomeA, combo.Text);
        Assert.Equal(fixture.HomeA, fixture.Form.CaptureStorageSelection().CodexHome);

        SetField(fixture.Form, "_settings", new AppSettings
        {
            RecentCodexHomes = [fixture.HomeB, fixture.HomeA]
        });
        Invoke(fixture.Form, "ReloadRecentHomes");
        Dictionary<string, string> reordered = combo.Items.Cast<AutomationComboBoxItem>()
            .ToDictionary(item => item.Value, item => item.InstanceKey, StringComparer.OrdinalIgnoreCase);
        Dictionary<string, string> originalKeys = original.ToDictionary(
            item => item.Value,
            item => item.InstanceKey,
            StringComparer.OrdinalIgnoreCase);
        Assert.Equal(originalKeys.OrderBy(pair => pair.Key), reordered.OrderBy(pair => pair.Key));
    }

    [Fact]
    public void Manifest_DialogOwnershipAndExemptionsAreExplicitAndNarrow()
    {
        using JsonDocument manifest = LoadManifest();
        JsonElement root = manifest.RootElement;
        JsonElement[] dialogs = root.GetProperty("dialogs").EnumerateArray().ToArray();
        JsonElement[] nativePickers = dialogs
            .Where(dialog => dialog.GetProperty("kind").GetString() == "native-folder-picker")
            .ToArray();
        Assert.Equal(3, nativePickers.Length);
        Assert.All(nativePickers, dialog =>
        {
            Assert.Equal("windows-shell", dialog.GetProperty("owner").GetString());
            Assert.Equal("native-folder-picker-internals", dialog.GetProperty("exemptionId").GetString());
            Assert.NotEmpty(dialog.GetProperty("launchControlId").GetString()!);
        });

        JsonElement[] applicationDialogs = dialogs
            .Where(dialog => dialog.GetProperty("owner").GetString() == "application")
            .ToArray();
        Assert.NotEmpty(applicationDialogs);
        Assert.All(applicationDialogs, dialog =>
        {
            Assert.Equal("native-message-box-internals", dialog.GetProperty("exemptionId").GetString());
            Assert.NotEmpty(dialog.GetProperty("actions").EnumerateArray());
            Assert.NotEmpty(dialog.GetProperty("scenarioIds").EnumerateArray());
        });

        Dictionary<string, JsonElement> exemptions = root.GetProperty("exemptions")
            .EnumerateArray()
            .ToDictionary(item => item.GetProperty("id").GetString()!, StringComparer.Ordinal);
        Assert.Equal(3, exemptions.Count);
        Assert.Contains("native-folder-picker-internals", exemptions.Keys);
        Assert.Contains("native-message-box-internals", exemptions.Keys);
        Assert.Contains("framework-composite-internals", exemptions.Keys);
        Assert.Equal(3, exemptions["native-folder-picker-internals"]
            .GetProperty("doesNotExempt")
            .GetArrayLength());
    }

    [Fact]
    public void Manifest_IsEmbeddedAndCopiedBesideTheApplication()
    {
        Assembly assembly = typeof(MainForm).Assembly;
        Assert.Contains(GuiAutomationCatalog.ManifestResourceName, assembly.GetManifestResourceNames());

        string outputManifest = Path.Combine(
            Path.GetDirectoryName(assembly.Location)!,
            "Automation",
            "gui-automation-manifest.v0.4.json");
        Assert.True(File.Exists(outputManifest), $"Expected copied static manifest at {outputManifest}.");
    }

    private static JsonDocument LoadManifest()
    {
        Stream stream = typeof(MainForm).Assembly.GetManifestResourceStream(GuiAutomationCatalog.ManifestResourceName)
            ?? throw new InvalidOperationException("Embedded GUI automation manifest was not found.");
        return JsonDocument.Parse(stream);
    }

    private static IEnumerable<Control> EnumerateRuntimeDenominator(MainForm form)
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

    private static T Field<T>(MainForm form, string name) where T : class
    {
        return typeof(MainForm)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)?
            .GetValue(form) as T
            ?? throw new InvalidOperationException($"Unable to read {name}.");
    }

    private static void SetField<T>(MainForm form, string name, T value)
    {
        FieldInfo field = typeof(MainForm).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Unable to find {name}.");
        field.SetValue(form, value);
    }

    private static void Invoke(MainForm form, string name, params object[] arguments)
    {
        MethodInfo method = typeof(MainForm).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Unable to find {name}.");
        method.Invoke(form, arguments);
    }

    private sealed class IsolatedMainForm : IDisposable
    {
        private readonly string _root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-gui-manifest-{Guid.NewGuid():N}");

        internal IsolatedMainForm()
        {
            Directory.CreateDirectory(_root);
            HomeA = Path.Combine(_root, "home-a");
            HomeB = Path.Combine(_root, "home-b");
            Form = new MainForm(
                new ExecutionLogService(Path.Combine(_root, "logs")),
                new SettingsService(Path.Combine(_root, "settings.json")));
        }

        internal MainForm Form { get; }
        internal string HomeA { get; }
        internal string HomeB { get; }

        public void Dispose()
        {
            Form.Dispose();
            Directory.Delete(_root, recursive: true);
        }
    }
}
