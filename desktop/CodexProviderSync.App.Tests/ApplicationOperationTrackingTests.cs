using CodexProviderSync.Application;
using CodexProviderSync.App.Automation;
using CodexProviderSync.Core;
using Microsoft.Data.Sqlite;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Windows.Forms;

namespace CodexProviderSync.App.Tests;

public sealed class ApplicationOperationTrackingTests
{
    [Theory]
    [InlineData(GuiAutomationCatalog.Ids.RefreshStatus)]
    [InlineData(GuiAutomationCatalog.Ids.Execute)]
    [InlineData(GuiAutomationCatalog.Ids.Restore)]
    [InlineData(GuiAutomationCatalog.Ids.PruneBackups)]
    public void EveryBusinessButton_RequiresAnApplicationTraceWindow(string automationId)
    {
        Assert.True(MainForm.IsApplicationBoundAutomationId(automationId));
    }

    [Theory]
    [InlineData(GuiAutomationCatalog.Ids.AddManualProvider)]
    [InlineData(GuiAutomationCatalog.Ids.OpenBackupDirectory)]
    [InlineData(GuiAutomationCatalog.Ids.CheckUpdates)]
    public void UiAndSettingsButtons_AreExplicitlyNotApplicationBound(string automationId)
    {
        Assert.False(MainForm.IsApplicationBoundAutomationId(automationId));
    }

    [Fact]
    public void DryRunReadyToApply_IsNeverAcceptedAsGuiApplySuccess()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> dryRun = new(
            "dry-run-operation",
            ApplicationOperationKind.Sync,
            ApplicationOperationLifecycle.ReadyToApply,
            now,
            now,
            new ApplicationWriteResult<SyncResult>(null!, Applied: false, Result: null),
            [],
            [],
            []);

        Assert.False(GuiApplicationOutcomePolicy.IsAppliedSuccess(dryRun));
    }

    [Fact]
    public void PlanStale_IsTheOnlyBoundedGuiReplanSignal()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> stale = new(
            "stale-operation",
            ApplicationOperationKind.Sync,
            ApplicationOperationLifecycle.Rejected,
            now,
            now,
            null,
            [],
            [new ApplicationError("plan_stale", "changed")],
            []);
        ApplicationOutcome<ApplicationWriteResult<SyncResult>> busy = stale with
        {
            Errors = [new ApplicationError("target_busy", "busy")]
        };

        Assert.True(GuiApplicationOutcomePolicy.IsPlanStale(stale));
        Assert.False(GuiApplicationOutcomePolicy.IsPlanStale(busy));
    }

    [Fact]
    public async Task AsyncHandler_RetainsGuiCorrelation_AfterDispatcherScopeReturns()
    {
        ApplicationOperationTraceHub hub = new();
        DeferredStatusService inner = new();
        TrackedApplicationService tracked = new(inner, hub);
        Task handler;
        ApplicationInvocationWindow window;

        using (ApplicationOperationTraceHub.ApplicationInvocationScope scope =
            hub.BeginInvocation("request-1", GuiAutomationCatalog.Ids.RefreshStatus))
        {
            window = scope.Window;
            handler = RunHandlerAsync();
        }

        inner.Complete(CreateStatusOutcome("status-real-1"));
        await handler;
        IReadOnlyList<ApplicationOperationTraceRecord> operations =
            await window.WaitAsync(CancellationToken.None);

        ApplicationOperationTraceRecord operation = Assert.Single(operations);
        Assert.Equal("status-real-1", operation.OperationId);
        Assert.Equal(ApplicationOperationKind.Status, operation.Operation);
        Assert.Equal(ApplicationOperationLifecycle.Succeeded, operation.Lifecycle);

        async Task RunHandlerAsync()
        {
            await tracked.GetStatusAsync(new ApplicationStatusRequest("C:\\fixture"));
            hub.CompleteCurrentInvocation();
        }
    }

    [Fact]
    public void TraceSink_WritesRealApplicationIdentityLifecycleAndCausalRequest()
    {
        string root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-gui-trace-{Guid.NewGuid():N}");
        string path = Path.Combine(root, "trace.jsonl");
        try
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            GuiAutomationTraceSink sink = new(path);
            sink.Append(
                "request-apply",
                "ui.invoke",
                GuiAutomationCatalog.Ids.Execute,
                "Click",
                eventObserved: true,
                [new ApplicationOperationTraceRecord(
                    "apply-operation-1",
                    ApplicationOperationKind.Sync,
                    ApplicationOperationLifecycle.Succeeded,
                    now,
                    now,
                    [])]);

            using JsonDocument document = JsonDocument.Parse(File.ReadAllText(path));
            JsonElement rootElement = document.RootElement;
            Assert.Equal(2, rootElement.GetProperty("schemaVersion").GetInt32());
            Assert.Equal("request-apply", rootElement.GetProperty("requestId").GetString());
            Assert.True(rootElement.GetProperty("applicationOperationLinked").GetBoolean());
            Assert.Equal("apply-operation-1", rootElement.GetProperty("applicationOperationId").GetString());
            Assert.Equal("Sync", rootElement.GetProperty("applicationOperation").GetString());
            Assert.Equal("Succeeded", rootElement.GetProperty("applicationLifecycle").GetString());
            Assert.Equal(1, rootElement.GetProperty("applicationOperations").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RefreshButton_RealWinFormsEvent_UsesProductionApplicationAndCoreFixture()
    {
        string root = Path.Combine(
            Path.GetTempPath(),
            $"codex-provider-gui-core-event-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        IAppPathProvider paths = new IsolatedAppPathProvider(root);
        Directory.CreateDirectory(paths.DefaultCodexHome);
        Directory.CreateDirectory(paths.RequiredSqliteHomeOverride!);
        await File.WriteAllTextAsync(
            Path.Combine(paths.DefaultCodexHome, "config.toml"),
            "model_provider = \"openai\"\n");
        await CreateStateDatabaseAsync(Path.Combine(
            paths.RequiredSqliteHomeOverride!,
            "state_5.sqlite"));
        SettingsService settings = new(paths.SettingsPath);
        AutomationIsolation.PrepareSettings(settings, paths);

        TaskCompletionSource<JsonNode> completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread ui = new(() =>
        {
            try
            {
                using MainForm form = new(
                    new ExecutionLogService(paths.LogDirectory),
                    settings,
                    paths: paths,
                    platformBoundary: new IsolatedAppPlatformBoundary(paths));
                GuiAutomationDispatcher dispatcher = new(
                    form,
                    new GuiAutomationTraceSink(paths.AutomationTracePath!));
                form.Shown += async (_, _) =>
                {
                    try
                    {
                        Button refresh = form.Controls
                            .Find(GuiAutomationCatalog.Ids.RefreshStatus, true)
                            .OfType<Button>()
                            .Single();
                        Label state = form.Controls
                            .Find(GuiAutomationCatalog.Ids.OperationState, true)
                            .OfType<Label>()
                            .Single();
                        RichTextBox status = form.Controls
                            .Find(GuiAutomationCatalog.Ids.StatusOutput, true)
                            .OfType<RichTextBox>()
                            .Single();
                        DateTimeOffset readyDeadline = DateTimeOffset.UtcNow.AddSeconds(5);
                        while ((!refresh.Enabled || state.Text != "就绪" || status.TextLength == 0)
                            && DateTimeOffset.UtcNow < readyDeadline)
                        {
                            await Task.Delay(25);
                        }
                        Assert.True(
                            refresh.Enabled && status.TextLength > 0,
                            "Initial real Application status refresh did not become ready.");
                        GuiAutomationRequest request = GuiAutomationRequest.Parse(
                            "{\"id\":\"real-refresh\",\"method\":\"ui.invoke\",\"params\":{\"automationId\":\"status.refresh\"}}");
                        JsonNode result = (await dispatcher.DispatchAsync(
                            request,
                            CancellationToken.None))!;
                        completion.TrySetResult(result);
                    }
                    catch (Exception error)
                    {
                        completion.TrySetException(error);
                    }
                    finally
                    {
                        form.Close();
                    }
                };
                System.Windows.Forms.Application.Run(form);
            }
            catch (Exception error)
            {
                completion.TrySetException(error);
            }
        });
        ui.SetApartmentState(ApartmentState.STA);
        ui.Start();

        try
        {
            JsonNode result = await completion.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(result["applicationOperationLinked"]!.GetValue<bool>());
            Assert.Equal("Status", result["applicationOperation"]!.GetValue<string>());
            Assert.Equal("Succeeded", result["applicationLifecycle"]!.GetValue<string>());
            JsonArray operations = result["applicationOperations"]!.AsArray();
            JsonNode operation = Assert.Single(operations)!;
            Assert.Equal("Status", operation["operation"]!.GetValue<string>());
            Assert.Equal("Succeeded", operation["lifecycle"]!.GetValue<string>());

            string trace = await File.ReadAllTextAsync(paths.AutomationTracePath!);
            Assert.Contains("\"requestId\":\"real-refresh\"", trace, StringComparison.Ordinal);
            Assert.Contains("\"applicationOperationLinked\":true", trace, StringComparison.Ordinal);
        }
        finally
        {
            if (!ui.Join(TimeSpan.FromSeconds(5)))
            {
                throw new TimeoutException("The isolated WinForms fixture did not close.");
            }
            SqliteConnection.ClearAllPools();
            Directory.Delete(root, recursive: true);
        }
    }

    private static async Task CreateStateDatabaseAsync(string path)
    {
        SQLitePCL.Batteries_V2.Init();
        await using SqliteConnection connection = new($"Data Source={path}");
        await connection.OpenAsync();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE threads (
              id TEXT PRIMARY KEY,
              model_provider TEXT,
              cwd TEXT NOT NULL DEFAULT '',
              archived INTEGER NOT NULL DEFAULT 0,
              first_user_message TEXT NOT NULL DEFAULT '',
              model TEXT
            );
            INSERT INTO threads (id, model_provider, cwd, archived, first_user_message, model)
            VALUES ('fixture-thread', 'openai', 'C:\fixture', 0, 'hello', NULL);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static ApplicationOutcome<StatusSnapshot> CreateStatusOutcome(string operationId)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        return new ApplicationOutcome<StatusSnapshot>(
            operationId,
            ApplicationOperationKind.Status,
            ApplicationOperationLifecycle.Succeeded,
            now,
            now,
            new StatusSnapshot
            {
                CodexHome = "C:\\fixture",
                CurrentProvider = new CurrentProviderInfo("openai", false),
                ConfiguredProviders = ["openai"],
                RolloutCounts = new ProviderCounts(),
                LockedRolloutFiles = [],
                UnreadableRolloutFiles = [],
                EncryptedContentCounts = new ProviderCounts(),
                SqliteCounts = null,
                BackupRoot = "C:\\fixture\\backups",
                BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
            },
            [],
            [],
            [new ApplicationLifecycleEvent(ApplicationOperationLifecycle.Succeeded, now)]);
    }

    private sealed class DeferredStatusService : IApplicationService
    {
        private readonly TaskCompletionSource<ApplicationOutcome<StatusSnapshot>> _status =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void Complete(ApplicationOutcome<StatusSnapshot> outcome) =>
            _status.TrySetResult(outcome);

        public Task<ApplicationOutcome<StatusSnapshot>> GetStatusAsync(
            ApplicationStatusRequest request,
            CancellationToken cancellationToken = default) =>
            _status.Task.WaitAsync(cancellationToken);

        public Task<ApplicationOutcome<ApplicationDescription>> DescribeAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationOperationPlan>> CreatePlanAsync(CreateApplicationPlanRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SyncAsync(SyncApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<SyncResult>>> SwitchAsync(SwitchApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<RestoreResult>>> RestoreAsync(RestoreApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ApplicationOutcome<ApplicationWriteResult<BackupPruneResult>>> PruneAsync(PruneApplicationRequest request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
