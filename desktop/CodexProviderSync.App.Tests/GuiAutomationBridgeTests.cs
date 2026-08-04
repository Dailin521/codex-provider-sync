using System.Text.Json;
using System.Text.Json.Nodes;
using System.Windows.Forms;
using CodexProviderSync.App.Automation;
using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.App.Tests;

public sealed class GuiAutomationBridgeTests
{
    [Fact]
    public async Task Set_ChangesTheRealMainFormControl_RaisesItsEvent_AndWritesAnExplicitTrace()
    {
        using Fixture fixture = new();
        TextBox input = fixture.Form.Controls.Find(GuiAutomationCatalog.Ids.ManualProviderId, true)
            .OfType<TextBox>()
            .Single();
        int observedEvents = 0;
        input.TextChanged += (_, _) => observedEvents++;
        GuiAutomationRequest request = GuiAutomationRequest.Parse(
            "{\"id\":\"set-1\",\"method\":\"ui.set\",\"params\":{\"automationId\":\"provider.manualId\",\"value\":\"isolated-provider\"}}");

        JsonNode? result = await fixture.Dispatcher.DispatchAsync(
            request,
            CancellationToken.None);

        Assert.Equal("isolated-provider", input.Text);
        Assert.Equal(1, observedEvents);
        Assert.Equal("isolated-provider", result!["value"]!.GetValue<string>());
        string trace = File.ReadAllText(fixture.TracePath);
        Assert.Contains("\"automationId\":\"provider.manualId\"", trace, StringComparison.Ordinal);
        Assert.Contains("\"guiEvent\":\"TextChanged\"", trace, StringComparison.Ordinal);
        Assert.Contains("\"eventObserved\":true", trace, StringComparison.Ordinal);
        Assert.Contains("\"applicationOperationLinked\":false", trace, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DescribeSnapshotAndGet_AreBoundToTheManifestAndRuntimeDenominator()
    {
        using Fixture fixture = new();
        JsonNode? describe = await fixture.Dispatch("ui.describe");
        JsonNode? snapshot = await fixture.Dispatch("ui.snapshot");
        JsonNode? get = await fixture.Dispatch(
            "ui.get",
            "{\"automationId\":\"state.operation\"}");

        Assert.Equal("0.4", describe!["schemaVersion"]!.GetValue<string>());
        Assert.Equal(31, snapshot!["controls"]!.AsArray().Count);
        Assert.Equal("state.operation", get!["automationId"]!.GetValue<string>());
        Assert.Equal("就绪", get["value"]!.GetValue<string>());
    }

    [Fact]
    public async Task UnknownMethodAndUnknownControl_AreRejected()
    {
        Assert.Throws<InvalidDataException>(() => GuiAutomationRequest.Parse(
            "{\"id\":\"bad\",\"method\":\"application.sync\"}"));

        using Fixture fixture = new();
        GuiAutomationRequest request = GuiAutomationRequest.Parse(
            "{\"id\":\"missing\",\"method\":\"ui.get\",\"params\":{\"automationId\":\"missing.control\"}}");
        await Assert.ThrowsAsync<KeyNotFoundException>(() => fixture.Dispatcher.DispatchAsync(
            request,
            CancellationToken.None));
    }

    [Fact]
    public async Task Set_RejectsDisabledRealControl()
    {
        using Fixture fixture = new();
        TextBox input = fixture.Form.Controls.Find(GuiAutomationCatalog.Ids.ManualProviderId, true)
            .OfType<TextBox>()
            .Single();
        input.Enabled = false;

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Dispatch(
                "ui.set",
                "{\"automationId\":\"provider.manualId\",\"value\":\"blocked\"}"));

        Assert.Contains("disabled", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(string.Empty, input.Text);
    }

    [Fact]
    public async Task AutomationStorageInputs_CannotEscapeTheIsolationRoot()
    {
        using Fixture fixture = new();
        string outside = Path.Combine(Path.GetDirectoryName(fixture.Root)!, "real-codex-home");
        string json = JsonSerializer.Serialize(new
        {
            id = "escape",
            method = "ui.set",
            @params = new
            {
                automationId = GuiAutomationCatalog.Ids.CodexHome,
                value = outside
            }
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Dispatcher.DispatchAsync(
            GuiAutomationRequest.Parse(json),
            CancellationToken.None));
    }

    [Fact]
    public async Task DynamicProviderAndRecentHomeIds_SelectRealItemsAndRaiseRealEvents()
    {
        using Fixture fixture = new();
        AppController controller = Field<AppController>(fixture.Form, "_appController");
        controller.ApplyProviderOptions(
        [
            new ProviderOption
            {
                Id = "relay-a",
                Sources = [ProviderSource.Config]
            },
            new ProviderOption
            {
                Id = "relay-b",
                Sources = [ProviderSource.Manual],
                IsManual = true
            }
        ], "relay-a");
        Invoke(fixture.Form, "ReloadProviderList");

        ListView providers = fixture.Form.Controls.Find(GuiAutomationCatalog.Ids.ProviderList, true)
            .OfType<ListView>()
            .Single();
        _ = fixture.Form.Handle;
        _ = providers.Handle;
        ListViewItem relayB = providers.Items.Cast<ListViewItem>()
            .Single(item => string.Equals(item.Tag as string, "relay-b", StringComparison.Ordinal));
        int providerEvents = 0;
        providers.SelectedIndexChanged += (_, _) => providerEvents++;

        JsonNode? providerResult = await fixture.Dispatch(
            "ui.set",
            JsonSerializer.Serialize(new { automationId = relayB.Name, value = true }));

        Assert.True(relayB.Selected);
        Assert.True(providerEvents > 0);
        Assert.Equal("relay-b", controller.Snapshot.SelectedProviderId);
        Assert.True(providerResult!["selected"]!.GetValue<bool>());

        ComboBox recentHomes = fixture.Form.Controls.Find(GuiAutomationCatalog.Ids.CodexHome, true)
            .OfType<ComboBox>()
            .Single();
        AutomationComboBoxItem recent = GuiAutomationCatalog.RecentCodexHome(fixture.Root);
        recentHomes.Items.Add(recent);
        int recentEvents = 0;
        recentHomes.SelectedIndexChanged += (_, _) => recentEvents++;

        JsonNode? recentResult = await fixture.Dispatch(
            "ui.set",
            JsonSerializer.Serialize(new { automationId = recent.AutomationId, value = true }));

        Assert.Same(recent, recentHomes.SelectedItem);
        Assert.Equal(1, recentEvents);
        Assert.True(recentResult!["selected"]!.GetValue<bool>());
        string trace = File.ReadAllText(fixture.TracePath);
        Assert.Contains(relayB.Name, trace, StringComparison.Ordinal);
        Assert.Contains(recent.AutomationId, trace, StringComparison.Ordinal);
        Assert.Contains("SelectedIndexChanged", trace, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CancelledQueuedUiAction_CannotExecuteAfterCancellationIsReported()
    {
        Action? queued = null;
        int executions = 0;
        using CancellationTokenSource cancellation = new();
        Task<int> pending = GuiAutomationDispatcher.RunScheduledOnceAsync(
            callback => queued = callback,
            () => ++executions,
            cancellation.Token);

        Assert.NotNull(queued);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);

        queued!();
        Assert.Equal(0, executions);
    }

    private static T Field<T>(MainForm form, string name) where T : class
    {
        return (T)(typeof(MainForm)
            .GetField(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.GetValue(form)
            ?? throw new InvalidOperationException($"Missing MainForm field {name}."));
    }

    private static void Invoke(MainForm form, string name)
    {
        typeof(MainForm)
            .GetMethod(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?.Invoke(form, null);
    }

    private sealed class Fixture : IDisposable
    {
        private readonly string _root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-gui-bridge-{Guid.NewGuid():N}");
        private int _requestId;

        internal Fixture()
        {
            Directory.CreateDirectory(_root);
            IAppPathProvider paths = new IsolatedAppPathProvider(_root);
            TracePath = paths.AutomationTracePath!;
            Form = new MainForm(
                new ExecutionLogService(paths.LogDirectory),
                new SettingsService(paths.SettingsPath),
                paths: paths,
                platformBoundary: new IsolatedAppPlatformBoundary(paths));
            Dispatcher = new GuiAutomationDispatcher(Form, new GuiAutomationTraceSink(TracePath));
        }

        internal MainForm Form { get; }
        internal GuiAutomationDispatcher Dispatcher { get; }
        internal string TracePath { get; }
        internal string Root => _root;

        internal Task<JsonNode?> Dispatch(string method, string parameters = "{}")
        {
            string request = JsonSerializer.Serialize(new
            {
                id = $"request-{++_requestId}",
                method,
                @params = JsonDocument.Parse(parameters).RootElement
            });
            return Dispatcher.DispatchAsync(
                GuiAutomationRequest.Parse(request),
                CancellationToken.None);
        }

        public void Dispose()
        {
            Form.Dispose();
            Directory.Delete(_root, recursive: true);
        }
    }
}
