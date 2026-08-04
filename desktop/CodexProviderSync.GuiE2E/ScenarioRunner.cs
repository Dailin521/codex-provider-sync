using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text;

namespace CodexProviderSync.GuiE2E;

internal sealed partial class ScenarioRunner
{
    internal static IReadOnlySet<string> SupportedEvidenceReferences { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        "application-trace",
        "before-after-file-diff",
        "bridge-control-event",
        "isolated-failure-path",
        "isolated-platform-boundary",
        "native-folder-dialog",
        "native-message-box",
        "no-external-child-process",
        "no-network-or-external-process",
        "published-exe-sha256",
        "real-enabled-state",
        "real-window-lifecycle",
        "rendered-log-output",
        "rendered-status",
        "restart-persistence",
        "stable-dynamic-id",
        "visible-main-window",
        "visible-real-control-text"
    };
    private readonly GuiE2EOptions _options;
    private readonly IsolatedFixture _fixture;
    private readonly EvidenceDocument _evidence;
    private readonly IReadOnlyDictionary<string, string?> _environment;
    private readonly string _manifestJson;
    private readonly string _scenarioJson;
    private readonly HashSet<string> _coveredEntries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, VerifiedApplicationLink> _linkedRequests = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ScenarioState> _scenarios = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string[]> _scenarioEntries = new(StringComparer.Ordinal);
    private readonly HashSet<string> _nonGatingScenarioIds = new(StringComparer.Ordinal);
    private bool _coverageFinalized;
    private AppSession? _session;

    internal ScenarioRunner(
        GuiE2EOptions options,
        IsolatedFixture fixture,
        EvidenceDocument evidence,
        IReadOnlyDictionary<string, string?> environment,
        string manifestJson,
        string scenarioJson)
    {
        _options = options;
        _fixture = fixture;
        _evidence = evidence;
        _environment = environment;
        _manifestJson = manifestJson;
        _scenarioJson = scenarioJson;
        LoadScenarioContract();
    }

    internal async Task<string> RunAsync(FixtureSnapshot before, CancellationToken cancellationToken)
    {
        string lastToken = string.Empty;
        try
        {
            _session = await AppSession.StartAsync(
                _options,
                _fixture,
                _environment,
                generation: 1,
                cancellationToken);
            lastToken = _session.Token;
            _evidence.Executable["pid"] = _session.Process.Id;
            _evidence.Executable["mainWindow"] = $"0x{_session.MainWindow:X}";
            _evidence.Executable["visibleMainWindow"] = true;
            Pass("gui.launch", "Published Release EXE exposed a visible top-level window on the active desktop.");

            await VerifyManifestAndControlsAsync(cancellationToken);
            await ExerciseStatusAsync(cancellationToken);
            await ExerciseNativePickersAsync(cancellationToken);
            string? targetRow = await ExerciseProvidersAsync(cancellationToken);
            await ExerciseInputsAsync(cancellationToken);

            FixtureSnapshot afterSync = before;
            if (targetRow is not null)
            {
                afterSync = await ExerciseSyncAndSwitchAsync(targetRow, before, cancellationToken);
            }
            await ExerciseRestoreEntryAsync(cancellationToken);
            await ExercisePruneAndShellAsync(cancellationToken);
            await PreparePersistenceAsync(cancellationToken);

            await _session.ShutdownAsync(cancellationToken);
            Pass("gui.shutdown", "ui.shutdown closed the real published WinForms process.");
            await _session.DisposeAsync();
            _session = null;

            _session = await AppSession.StartAsync(
                _options,
                _fixture,
                _environment,
                generation: 2,
                cancellationToken);
            lastToken = _session.Token;
            await VerifyRestartPersistenceAsync(cancellationToken);
            await _session.ShutdownAsync(cancellationToken);
            await _session.DisposeAsync();
            _session = null;

            await CollectAndValidateTraceAsync(cancellationToken);
            FixtureSnapshot final = await _fixture.SnapshotAsync(cancellationToken);
            _evidence.FileDiffs.Add(Diff("initial-to-final", before, final));
            _evidence.FileDiffs.Add(Diff("post-sync-to-final", afterSync, final));
            FinalizeCoverage();
            return lastToken;
        }
        finally
        {
            if (_session is not null)
            {
                await _session.DisposeAsync();
                _session = null;
            }
        }
    }

    private GuiBridgeClient Bridge => _session?.Bridge
        ?? throw new InvalidOperationException("GUI session is not running.");

    private async Task VerifyManifestAndControlsAsync(CancellationToken cancellationToken)
    {
        BridgeResponse describe = await RequireResponseAsync("ui.describe", null, false, cancellationToken);
        JsonNode expected = JsonNode.Parse(_manifestJson)!;
        if (!JsonNode.DeepEquals(expected, describe.Result))
        {
            throw new InvalidDataException("Embedded ui.describe manifest differs from the published manifest file.");
        }
        _evidence.Manifest["embeddedMatchesPublished"] = true;

        BridgeResponse snapshot = await RequireResponseAsync("ui.snapshot", null, false, cancellationToken);
        JsonArray runtimeControls = snapshot.Result?["controls"]?.AsArray()
            ?? throw new InvalidDataException("ui.snapshot did not return controls.");
        HashSet<string> runtimeIds = runtimeControls
            .Select(node => node?["automationId"]?.GetValue<string>() ?? string.Empty)
            .ToHashSet(StringComparer.Ordinal);

        using JsonDocument manifest = JsonDocument.Parse(_manifestJson);
        List<string> staticIds =
        [
            manifest.RootElement.GetProperty("window").GetProperty("id").GetString()!
        ];
        staticIds.AddRange(manifest.RootElement.GetProperty("controls").EnumerateArray()
            .Select(entry => entry.GetProperty("id").GetString()!));
        if (!runtimeIds.SetEquals(staticIds))
        {
            throw new InvalidDataException(
                $"Runtime/manifest control mismatch. Missing [{string.Join(",", staticIds.Except(runtimeIds))}], "
                + $"extra [{string.Join(",", runtimeIds.Except(staticIds))}].");
        }

        foreach (string id in staticIds)
        {
            BridgeResponse response = await RequireResponseAsync(
                "ui.get",
                new JsonObject { ["automationId"] = id },
                false,
                cancellationToken);
            JsonObject value = response.Result?.AsObject()
                ?? throw new InvalidDataException($"ui.get {id} returned no control description.");
            _coveredEntries.Add(id);
            _evidence.Controls.Add(new()
            {
                ["automationId"] = id,
                ["requestId"] = response.Id,
                ["method"] = "ui.get",
                ["controlType"] = value["controlType"]?.GetValue<string>(),
                ["enabled"] = value["enabled"]?.GetValue<bool?>(),
                ["visible"] = value["visible"]?.GetValue<bool?>(),
                ["value"] = value["value"]?.DeepClone()
            });
        }

        PassMany("Real registered controls were read through the named-pipe bridge.",
            "gui.status.explicit-codex-home",
            "gui.status.explicit-sqlite-home",
            "gui.status.rendered-result",
            "gui.provider.manual-input",
            "gui.sync.mode",
            "gui.switch.mode",
            "gui.restore.include-config",
            "gui.restore.include-database",
            "gui.restore.include-sessions",
            "gui.backup.retention",
            "gui.operation.ready-state",
            "gui.logs.rendered-output",
            "gui.warning.close-codex",
            "gui.warning.close-app-server");
    }

    private async Task ExerciseStatusAsync(CancellationToken cancellationToken)
    {
        BridgeResponse focus = await RequireResponseAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = "window.main" },
            false,
            cancellationToken);
        RecordControl("window.main", "ui.invoke", focus);
        Pass("gui.focus-existing-instance", "window.main invocation raised the real activation path.");

        BridgeResponse refresh = await RequireResponseAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = "status.refresh" },
            true,
            cancellationToken);
        RecordControl("status.refresh", "ui.invoke", refresh);
        Pass("gui.status.refresh", "Refresh Click completed through the shared Application status operation.");
        JsonNode? status = (await Bridge.GetAsync("status.output", cancellationToken))?["value"];
        if (status is null || !status.GetValue<string>().Contains("openai", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException("Rendered GUI status did not contain isolated fixture provider openai.");
        }
        await ExerciseExpectedStatusFailureAsync(cancellationToken);
    }

    private async Task ExerciseExpectedStatusFailureAsync(CancellationToken cancellationToken)
    {
        FixtureSnapshot before = await _fixture.SnapshotAsync(cancellationToken);
        FileStream configLock = new(
            _fixture.ConfigPath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.Asynchronous);
        try
        {
            Task<BridgeResponse> pending = Bridge.SendAsync(
                "ui.invoke",
                new JsonObject { ["automationId"] = "status.refresh" },
                cancellationToken);
            NativeDialogObservation? dialog = await NativeWindows.WaitForDialogAsync(
                _session!.Process.Id,
                _session.MainWindow,
                TimeSpan.FromSeconds(5),
                cancellationToken);
            if (dialog is null)
            {
                BridgeResponse response = await pending;
                RecordControl("status.refresh", "ui.invoke", response);
                throw new InvalidDataException(
                    "An exclusive lock on isolated config.toml did not show a real status failure MessageBox.");
            }
            RequirePendingModal(pending, "status failure MessageBox");
            DialogContractResult contract = DialogContractVerifier.Verify(
                "operation-failure", dialog.ClassName, dialog.Title, dialog.Text);
            if (!contract.Passed)
            {
                await RejectUnexpectedDialogAsync(
                    pending,
                    "status.refresh",
                    "operation-failure",
                    dialog,
                    contract.Error ?? "Unknown status failure dialog contract failure.",
                    cancellationToken);
            }
            string screenshotPath = Path.Combine(
                _fixture.Root,
                "automation",
                "screenshots",
                $"{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}-status-config-lock-failure.png");
            NativeWindows.CaptureWindow(dialog.Handle, screenshotPath);
            string screenshotSha256 = await Hashing.Sha256FileAsync(screenshotPath, cancellationToken);
            RecordDialog(
                "dialog.operationFailure",
                "status-refresh-config-lock",
                dialog,
                "acknowledge",
                screenshotPath,
                screenshotSha256);
            NativeWindows.ClickDialogButton(dialog.Handle, 1);
            BridgeResponse completed = await pending;
            RecordControl("status.refresh", "ui.invoke", completed);
            if (!completed.Ok)
            {
                throw new InvalidDataException(
                    $"Status failure GUI event returned a bridge failure [{completed.ErrorCode}]: {completed.ErrorMessage}");
            }
            RequireApplicationLink(
                completed,
                "status.refresh",
                "Status",
                "Failed",
                "operation_failed");
            _coveredEntries.Add("dialog.operationFailure");
            Pass("gui.status.refresh-failure",
                "An exclusive lock on isolated config.toml produced a real failure MessageBox and Failed/operation_failed Application trace; the external driver captured and acknowledged it.");
        }
        finally
        {
            await configLock.DisposeAsync();
        }

        FixtureSnapshot restored = await _fixture.SnapshotAsync(cancellationToken);
        if (!SameFixtureState(before, restored))
        {
            throw new InvalidDataException("Status failure injection did not restore the isolated fixture exactly.");
        }
        BridgeResponse recovery = await RequireResponseAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = "status.refresh" },
            true,
            cancellationToken);
        RecordControl("status.refresh", "ui.invoke", recovery);
    }

    private async Task<string?> ExerciseProvidersAsync(CancellationToken cancellationToken)
    {
        JsonArray rows = (await Bridge.GetAsync("provider.list", cancellationToken))?["value"]?.AsArray()
            ?? throw new InvalidDataException("Provider list did not expose dynamic rows.");
        JsonNode? target = rows.FirstOrDefault(node =>
            string.Equals(node?["text"]?.GetValue<string>(), "apigather", StringComparison.OrdinalIgnoreCase));
        if (target is null)
        {
            BlockMany("Isolated config provider apigather was not rendered as a real provider row.",
                "gui.provider.select", "gui.provider.dynamic-row-identity", "gui.provider.dynamic-row-selection");
            return null;
        }
        string rowId = target["automationId"]!.GetValue<string>();
        BridgeResponse get = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = rowId }, false, cancellationToken);
        BridgeResponse select = await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = rowId, ["value"] = true }, false, cancellationToken);
        RecordControl(rowId, "ui.get", get);
        RecordControl(rowId, "ui.set", select);
        _coveredEntries.Add("provider.row");
        PassMany("Stable dynamic provider row was read and selected through the real ListView event.",
            "gui.provider.select", "gui.provider.selection-state", "gui.provider.dynamic-row-identity", "gui.provider.dynamic-row-selection");

        string originalRecentId = "storage.codexHome.recent:"
            + StableInstanceKey(_fixture.CodexHome.ToUpperInvariant());
        string alternateRecentId = "storage.codexHome.recent:"
            + StableInstanceKey(_fixture.PickerCodexHome.ToUpperInvariant());
        BridgeResponse originalRecent = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = originalRecentId }, false, cancellationToken);
        BridgeResponse alternateRecent = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = alternateRecentId }, false, cancellationToken);
        RecordControl(originalRecentId, "ui.get", originalRecent);
        RecordControl(alternateRecentId, "ui.get", alternateRecent);

        BridgeResponse selectAlternate = await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = alternateRecentId, ["value"] = true }, false, cancellationToken);
        RecordControl(alternateRecentId, "ui.set", selectAlternate);
        await RequireStorageControlValueAsync("storage.codexHome", _fixture.PickerCodexHome, cancellationToken);

        BridgeResponse selectOriginal = await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = originalRecentId, ["value"] = true }, false, cancellationToken);
        RecordControl(originalRecentId, "ui.set", selectOriginal);
        await RequireStorageControlValueAsync("storage.codexHome", _fixture.CodexHome, cancellationToken);
        BridgeResponse recentRefresh = await RequireResponseAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = "status.refresh" },
            true,
            cancellationToken);
        RecordControl("status.refresh", "ui.invoke", recentRefresh);
        await RequireStoragePickerResultAsync("storage.codexHome", _fixture.CodexHome, cancellationToken);

        _coveredEntries.Add("storage.codexHome.recent");
        PassMany(
            "Stable alternate and original recent-home rows were read and selected in sequence; both real ComboBox events fired and final refreshed storage state returned to the original isolated home.",
            "gui.settings.recent-home-identity", "gui.settings.recent-home-selection");

        await SetChangedAsync("provider.manualId", "validation-probe", cancellationToken);
        await SetChangedAsync("provider.manualId", string.Empty, cancellationToken);
        NativeDialogObservation? validation = await InvokeWithDialogAsync(
            "provider.addManual", 1, "validation", cancellationToken);
        if (validation is not null)
        {
            _coveredEntries.Add("dialog.validation");
            Pass("gui.provider.add-manual-validation", "Real MessageBox validation was observed and acknowledged externally.");
        }

        await SetChangedAsync("provider.manualId", "manual-e2e", cancellationToken);
        BridgeResponse add = await RequireResponseAsync(
            "ui.invoke", new JsonObject { ["automationId"] = "provider.addManual" }, false, cancellationToken);
        RecordControl("provider.addManual", "ui.invoke", add);
        string manualRow = await WaitForDynamicRowAsync("manual-e2e", cancellationToken);
        Pass("gui.provider.add-manual", "Manual provider was added by the real button/event/settings path.");
        BridgeResponse selectManual = await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = manualRow, ["value"] = true }, false, cancellationToken);
        RecordControl(manualRow, "ui.set", selectManual);
        BridgeResponse remove = await RequireResponseAsync(
            "ui.invoke", new JsonObject { ["automationId"] = "provider.removeManual" }, false, cancellationToken);
        RecordControl("provider.removeManual", "ui.invoke", remove);
        await WaitForDynamicRowMissingAsync(manualRow, cancellationToken);
        Pass("gui.provider.remove-manual", "A real dynamic manual row was selected and removed through the real button/settings path.");
        return rowId;
    }

    private async Task ExerciseInputsAsync(CancellationToken cancellationToken)
    {
        await SetChangedAsync("execution.updateConfig", true, cancellationToken);
        await SetChangedAsync("execution.model.keepCurrent", true, cancellationToken);
        await SetChangedAsync("execution.model.custom", true, cancellationToken);
        await SetChangedAsync("execution.customModel", "gpt-e2e-custom", cancellationToken);
        await SetChangedAsync("execution.model.followProvider", true, cancellationToken);
        await SetChangedAsync("execution.updateConfig", false, cancellationToken);
        await SetChangedAsync("restore.includeConfig", false, cancellationToken);
        await SetChangedAsync("restore.includeConfig", true, cancellationToken);
        await SetChangedAsync("restore.includeDatabase", false, cancellationToken);
        await SetChangedAsync("restore.includeDatabase", true, cancellationToken);
        await SetChangedAsync("restore.includeSessions", false, cancellationToken);
        await SetChangedAsync("restore.includeSessions", true, cancellationToken);
        await SetChangedAsync("backup.retentionCount", 7, cancellationToken);
        PassMany("Real CheckedChanged/TextChanged/ValueChanged events exercised operation inputs.",
            "gui.switch.model-follow-provider",
            "gui.switch.model-keep-current",
            "gui.switch.model-custom-mode",
            "gui.switch.model-custom-value",
            "gui.restore.include-config",
            "gui.restore.include-database",
            "gui.restore.include-sessions",
            "gui.backup.retention");
    }

    private async Task ExerciseNativePickersAsync(CancellationToken cancellationToken)
    {
        await ProbeFolderPickerRoundTripAsync(
            "storage.codexHome.browse",
            "storage.codexHome",
            "dialog.storage.codexHomePicker",
            "gui.status.browse-codex-home",
            _fixture.PickerCodexHome,
            _fixture.CodexHome,
            cancellationToken);
        await ProbeFolderPickerRoundTripAsync(
            "storage.sqliteHome.browse",
            "storage.sqliteHome",
            "dialog.storage.sqliteHomePicker",
            "gui.status.browse-sqlite-home",
            _fixture.PickerSqliteHome,
            _fixture.SqliteHome,
            cancellationToken);
    }

    private async Task<FixtureSnapshot> ExerciseSyncAndSwitchAsync(
        string targetRow,
        FixtureSnapshot before,
        CancellationToken cancellationToken)
    {
        await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = targetRow, ["value"] = true }, false, cancellationToken);
        await SetChangedAsync("execution.updateConfig", false, cancellationToken);

        NativeDialogObservation? cancelled = await InvokeWithDialogAsync(
            "operation.execute", 2, "sync-cancel", cancellationToken);
        if (cancelled is null)
        {
            BlockMany("Sync confirmation MessageBox was not externally observable.",
                "gui.sync.confirm-close-codex", "gui.sync.execute", "gui.switch.execute");
            return before;
        }
        _coveredEntries.Add("dialog.confirmation");
        Pass("gui.sync.confirm-close-codex", "Real close-Codex confirmation was observed; Cancel left fixture files unchanged.");
        FixtureSnapshot afterCancel = await _fixture.SnapshotAsync(cancellationToken);
        if (!SameFixtureState(before, afterCancel))
        {
            throw new InvalidDataException("Cancelling the real sync confirmation changed isolated fixture files.");
        }

        BridgeResponse? sync = await InvokeWithDialogResponseAsync(
            "operation.execute", 1, "sync-apply", cancellationToken);
        if (sync is null)
        {
            Block("gui.sync.execute", "Sync apply confirmation could not be driven externally.");
            return before;
        }
        RequireApplicationLink(sync, "operation.execute", "Sync");
        FixtureSnapshot afterSync = await _fixture.SnapshotAsync(cancellationToken);
        _evidence.FileDiffs.Add(Diff("sync", before, afterSync));
        if (afterSync.ConfigProvider != "openai"
            || afterSync.RolloutProvider != "apigather"
            || afterSync.DatabaseProvider != "apigather"
            || afterSync.ManagedBackupCount < 1)
        {
            throw new InvalidDataException("Real GUI sync did not produce the expected isolated rollout/SQLite/backup diff.");
        }
        Pass("gui.sync.execute", "Real Execute Click synced rollout and SQLite, preserved config, and created a backup.");

        await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = targetRow, ["value"] = true }, false, cancellationToken);
        await SetChangedAsync("execution.updateConfig", true, cancellationToken);
        await SetChangedAsync("execution.model.followProvider", true, cancellationToken);
        BridgeResponse? switched = await InvokeWithDialogResponseAsync(
            "operation.execute", 1, "switch-apply", cancellationToken);
        if (switched is not null)
        {
            RequireApplicationLink(switched, "operation.execute", "Switch");
            FixtureSnapshot afterSwitch = await _fixture.SnapshotAsync(cancellationToken);
            _evidence.FileDiffs.Add(Diff("switch", afterSync, afterSwitch));
            if (afterSwitch.ConfigProvider != "apigather")
            {
                throw new InvalidDataException("Real GUI switch did not update isolated config.toml.");
            }
            Pass("gui.switch.execute", "Real Execute Click switched config and synchronized isolated metadata.");
            return afterSwitch;
        }
        Block("gui.switch.execute", "Switch confirmation could not be driven externally.");
        return afterSync;
    }

    private async Task ExerciseRestoreEntryAsync(CancellationToken cancellationToken)
    {
        string? backup = _fixture.ManagedBackups().FirstOrDefault();
        if (backup is null)
        {
            BlockMany("No managed backup was produced by the preceding real GUI sync/switch operations.",
                "gui.restore.select-backup", "gui.restore.execute", "gui.restore.cancel");
            return;
        }

        FixtureSnapshot beforeCancel = await _fixture.SnapshotAsync(cancellationToken);
        BridgeResponse? cancelled = await DriveRestoreAsync(backup, acceptConfirmation: false, cancellationToken);
        if (cancelled is null)
        {
            string blocker = "Restore picker/confirmation could not be driven through the real native modal chain.";
            _evidence.Blockers.Add(blocker);
            BlockMany(blocker,
                "gui.restore.select-backup", "gui.restore.execute", "gui.restore.cancel",
                "gui.restore.confirm-targets", "gui.restore.confirm-close-codex");
            throw new InvalidOperationException(blocker);
        }
        FixtureSnapshot afterCancel = await _fixture.SnapshotAsync(cancellationToken);
        if (!SameFixtureState(beforeCancel, afterCancel))
        {
            throw new InvalidDataException("Cancelling real restore confirmation changed isolated files.");
        }
        PassMany("Real restore picker selected an isolated backup and real confirmation Cancel preserved all files.",
            "gui.restore.select-backup", "gui.restore.cancel", "gui.restore.confirm-targets");

        BridgeResponse? applied = await DriveRestoreAsync(backup, acceptConfirmation: true, cancellationToken);
        if (applied is null)
        {
            Block("gui.restore.execute", "Restore apply native modal chain failed.");
            throw new InvalidOperationException(
                _evidence.Blockers.LastOrDefault()
                ?? "Restore apply failed through the real GUI modal/Application path.");
        }
        RequireApplicationLink(applied, "restore.execute", "Restore");
        FixtureSnapshot afterRestore = await _fixture.SnapshotAsync(cancellationToken);
        _evidence.FileDiffs.Add(Diff("restore", beforeCancel, afterRestore));
        if (afterRestore.ConfigProvider != "openai"
            || afterRestore.RolloutProvider != "legacy-e2e"
            || afterRestore.DatabaseProvider != "legacy-e2e"
            || afterRestore.DatabaseModel != "gpt-e2e-old")
        {
            throw new InvalidDataException("Real GUI restore did not restore the earliest isolated backup contents.");
        }
        PassMany("Real restore picker, confirmations, shared Application apply, and file diff restored the isolated backup.",
            "gui.restore.execute", "gui.restore.confirm-close-codex");
    }

    private async Task<BridgeResponse?> DriveRestoreAsync(
        string backup,
        bool acceptConfirmation,
        CancellationToken cancellationToken)
    {
        Task<BridgeResponse> pending = Bridge.SendAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = "restore.execute" },
            cancellationToken);
        NativeDialogObservation? picker = await NativeWindows.WaitForDialogAsync(
            _session!.Process.Id, _session.MainWindow, TimeSpan.FromSeconds(5), cancellationToken);
        if (picker is null)
        {
            BridgeResponse response = await pending;
            RecordControl("restore.execute", "ui.invoke", response);
            return null;
        }
        if (IsLocationUnavailable(picker))
        {
            RecordDialog("dialog.shell.locationUnavailable", "restore-picker-shell-error", picker, "acknowledge");
            NativeWindows.ClickDialogButton(picker.Handle, 1);
            NativeDialogObservation? underlying = await NativeWindows.WaitForDialogAsync(
                _session.Process.Id, _session.MainWindow, TimeSpan.FromSeconds(2), cancellationToken);
            if (underlying is not null)
            {
                NativeWindows.ClickDialogButton(underlying.Handle, 2);
            }
            _ = await pending;
            _evidence.Blockers.Add("Windows shell reported Location unavailable inside the sanitized profile; restore picker evidence is invalid.");
            return null;
        }
        RequirePendingModal(pending, "restore FolderBrowserDialog");
        RecordDialog("dialog.restore.backupPicker", "restore-picker", picker, "accept");
        _coveredEntries.Add("dialog.restore.backupPicker");
        if (!NativeWindows.TryEnterFolderAndAccept(picker.Handle, backup))
        {
            NativeWindows.ClickDialogButton(picker.Handle, 2);
            _ = await pending;
            return null;
        }

        NativeDialogObservation? confirmation = await NativeWindows.WaitForDialogAsync(
            _session.Process.Id, _session.MainWindow, TimeSpan.FromSeconds(5), cancellationToken);
        if (confirmation is null)
        {
            _ = await pending;
            return null;
        }
        RequirePendingModal(pending, "restore target confirmation MessageBox");
        DialogContractResult targetContract = DialogContractVerifier.Verify(
            "restore-targets", confirmation.ClassName, confirmation.Title, confirmation.Text);
        if (!targetContract.Passed)
        {
            await RejectUnexpectedDialogAsync(
                pending,
                "restore.execute",
                "restore-targets",
                confirmation,
                targetContract.Error ?? "Unknown restore-target dialog contract failure.",
                cancellationToken);
        }
        RecordDialog("dialog.confirmation", "restore-targets", confirmation, acceptConfirmation ? "accept" : "cancel");
        _coveredEntries.Add("dialog.confirmation");
        NativeWindows.ClickDialogButton(confirmation.Handle, acceptConfirmation ? 1 : 2);
        if (!acceptConfirmation)
        {
            BridgeResponse cancelled = await pending;
            RecordControl("restore.execute", "ui.invoke", cancelled);
            return cancelled.Ok ? cancelled : null;
        }

        NativeDialogObservation? closeCodex = await NativeWindows.WaitForDialogAsync(
            _session.Process.Id, _session.MainWindow, TimeSpan.FromSeconds(5), cancellationToken);
        if (closeCodex is null)
        {
            _ = await pending;
            return null;
        }
        RequirePendingModal(pending, "restore close-Codex confirmation MessageBox");
        DialogContractResult closeContract = DialogContractVerifier.Verify(
            "restore-close-codex", closeCodex.ClassName, closeCodex.Title, closeCodex.Text);
        if (!closeContract.Passed)
        {
            await RejectUnexpectedDialogAsync(
                pending,
                "restore.execute",
                "restore-close-codex",
                closeCodex,
                closeContract.Error ?? "Unknown restore-close-Codex dialog contract failure.",
                cancellationToken);
        }
        RecordDialog("dialog.confirmation", "restore-close-codex", closeCodex, "accept");
        NativeWindows.ClickDialogButton(closeCodex.Handle, 1);
        (BridgeResponse applied, bool failureDialog) = await AwaitAfterConfirmationAsync(
            pending,
            "restore.execute",
            "restore-apply",
            closeCodex.Handle,
            cancellationToken);
        return applied.Ok && !failureDialog ? applied : null;
    }

    private async Task ExercisePruneAndShellAsync(CancellationToken cancellationToken)
    {
        await SetChangedAsync("backup.retentionCount", 1, cancellationToken);
        string backupRoot = Path.Combine(_fixture.CodexHome, "backups_state", "provider-sync");
        string unmanagedDirectory = Path.Combine(backupRoot, "non-managed-e2e-sentinel");
        string unmanagedSentinel = Path.Combine(unmanagedDirectory, "must-survive-prune.txt");
        Directory.CreateDirectory(unmanagedDirectory);
        await File.WriteAllTextAsync(
            unmanagedSentinel,
            "not a managed provider-sync backup" + Environment.NewLine,
            cancellationToken);
        string unmanagedSentinelSha256 = await Hashing.Sha256FileAsync(unmanagedSentinel, cancellationToken);
        string[] managedBefore = _fixture.ManagedBackups().ToArray();
        if (managedBefore.Length <= 1)
        {
            throw new InvalidDataException(
                $"Prune E2E requires more than one managed backup, observed {managedBefore.Length}.");
        }
        string expectedNewest = managedBefore[^1];
        FixtureSnapshot beforePrune = await _fixture.SnapshotAsync(cancellationToken);
        NativeDialogObservation? pruneCancel = await InvokeWithDialogAsync(
            "backups.prune", 2, "prune-cancel", cancellationToken);
        if (pruneCancel is null)
        {
            throw new InvalidOperationException("Prune cancellation confirmation could not be driven externally.");
        }
        string[] managedAfterCancel = _fixture.ManagedBackups().ToArray();
        FixtureSnapshot afterCancel = await _fixture.SnapshotAsync(cancellationToken);
        await PruneEvidenceContract.VerifySentinelAsync(
            unmanagedSentinel, unmanagedSentinelSha256, cancellationToken);
        if (!managedBefore.SequenceEqual(managedAfterCancel, StringComparer.Ordinal)
            || !SameFixtureState(beforePrune, afterCancel))
        {
            throw new InvalidDataException(
                "Cancelling the real prune confirmation changed managed backups, fixture files, or the unmanaged sentinel.");
        }
        _coveredEntries.Add("dialog.confirmation");
        PassMany("Real prune MessageBox was observed and Cancel preserved every managed backup and the non-managed sentinel.",
            "gui.backup.prune-cancel", "gui.backup.prune-confirmation");

        BridgeResponse? prune = await InvokeWithDialogResponseAsync(
            "backups.prune", 1, "prune-apply", cancellationToken);
        if (prune is null)
        {
            throw new InvalidOperationException("Prune apply confirmation could not be driven externally.");
        }
        RequireApplicationLink(prune, "backups.prune", "Prune");
        FixtureSnapshot afterPrune = await _fixture.SnapshotAsync(cancellationToken);
        string[] managedAfter = _fixture.ManagedBackups().ToArray();
        PruneRemovalEvidence removal = PruneEvidenceContract.VerifyManagedRemoval(
            managedBefore, managedAfter, expectedNewest);
        await PruneEvidenceContract.VerifySentinelAsync(
            unmanagedSentinel, unmanagedSentinelSha256, cancellationToken);
        Dictionary<string, object?> pruneDiff = Diff("prune", beforePrune, afterPrune);
        pruneDiff["managedBefore"] = managedBefore;
        pruneDiff["managedAfter"] = managedAfter;
        pruneDiff["expectedNewestRetained"] = expectedNewest;
        pruneDiff["managedCountDecreased"] = managedAfter.Length < managedBefore.Length;
        pruneDiff["removedManagedPaths"] = removal.RemovedPaths;
        pruneDiff["removedManagedPathsAbsent"] = removal.RemovedPaths.All(path => !Directory.Exists(path));
        pruneDiff["unmanagedSentinel"] = unmanagedSentinel;
        pruneDiff["unmanagedSentinelSha256"] = unmanagedSentinelSha256;
        pruneDiff["unmanagedSentinelPreserved"] = true;
        _evidence.FileDiffs.Add(pruneDiff);

        BridgeResponse logs = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = "logs.output" }, false, cancellationToken);
        BridgeResponse operation = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = "state.operation" }, false, cancellationToken);
        RecordControl("logs.output", "ui.get", logs);
        RecordControl("state.operation", "ui.get", operation);
        string logText = logs.Result?["value"]?.GetValue<string>() ?? string.Empty;
        string operationText = operation.Result?["value"]?.GetValue<string>() ?? string.Empty;
        if (!logText.Contains("旧备份清理完成", StringComparison.Ordinal)
            || !string.Equals(operationText, "就绪", StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Prune storage state changed, but the rendered log/operation-ready state was inconsistent.");
        }
        Pass("gui.backup.prune",
            "Real prune Click/confirmation/Application path reduced managed backups to one, retained the newest, preserved unmanaged data, and rendered ready/log state.");

        IReadOnlySet<int> beforeChildren = NativeWindows.DescendantProcessIds(_session!.Process.Id);
        BridgeResponse backupOpen = await RequireResponseAsync(
            "ui.invoke", new JsonObject { ["automationId"] = "backups.openDirectory" }, false, cancellationToken);
        BridgeResponse logOpen = await RequireResponseAsync(
            "ui.invoke", new JsonObject { ["automationId"] = "logs.openDirectory" }, false, cancellationToken);
        await Task.Delay(250, cancellationToken);
        IReadOnlySet<int> afterChildren = NativeWindows.DescendantProcessIds(_session.Process.Id);
        int[] launched = afterChildren.Except(beforeChildren).ToArray();
        RecordControl("backups.openDirectory", "ui.invoke", backupOpen);
        RecordControl("logs.openDirectory", "ui.invoke", logOpen);
        if (launched.Length > 0)
        {
            throw new InvalidOperationException(
                $"UI-only isolated shell buttons unexpectedly launched child processes: {string.Join(",", launched)}.");
        }
        PassMany("Real shell button Click events reached the isolated boundary without starting a child process.",
            "gui.backup.open-directory", "gui.logs.open-directory");

        IReadOnlySet<int> updateChildrenBefore = NativeWindows.DescendantProcessIds(_session.Process.Id);
        BridgeResponse? update = await InvokeWithDialogResponseAsync(
            "updates.check", 1, "update-no-update", cancellationToken);
        IReadOnlySet<int> updateChildrenAfter = NativeWindows.DescendantProcessIds(_session.Process.Id);
        if (update is not null && !updateChildrenAfter.Except(updateChildrenBefore).Any())
        {
            _coveredEntries.Add("dialog.update");
            PassMany("Real update Click reached deterministic isolated no-update boundary, showed a real MessageBox, and launched no process.",
                "gui.update.check", "gui.update.no-update");
        }
        else
        {
            BlockMany("Deterministic isolated update no-update MessageBox was not proven.",
                "gui.update.check", "gui.update.no-update");
        }
    }

    private async Task PreparePersistenceAsync(CancellationToken cancellationToken)
    {
        await SetChangedAsync("backup.retentionCount", 7, cancellationToken);
        await SetChangedAsync("provider.manualId", "manual-persist-e2e", cancellationToken);
        BridgeResponse add = await RequireResponseAsync(
            "ui.invoke", new JsonObject { ["automationId"] = "provider.addManual" }, false, cancellationToken);
        RecordControl("provider.addManual", "ui.invoke", add);
        string rowId = await WaitForDynamicRowAsync("manual-persist-e2e", cancellationToken);
        BridgeResponse select = await RequireResponseAsync(
            "ui.set", new JsonObject { ["automationId"] = rowId, ["value"] = true }, false, cancellationToken);
        RecordControl(rowId, "ui.set", select);
        await Task.Delay(500, cancellationToken);
    }

    private async Task VerifyRestartPersistenceAsync(CancellationToken cancellationToken)
    {
        JsonNode retention = await Bridge.GetAsync("backup.retentionCount", cancellationToken)
            ?? throw new InvalidDataException("Retention control missing after restart.");
        decimal value = retention["value"]!.GetValue<decimal>();
        string providerRow = await WaitForDynamicRowAsync("manual-persist-e2e", cancellationToken);
        JsonNode providerState = await Bridge.GetAsync("state.selectedProvider", cancellationToken)
            ?? throw new InvalidDataException("Selected-provider state missing after restart.");
        JsonNode persistedRow = await Bridge.GetAsync(providerRow, cancellationToken)
            ?? throw new InvalidDataException("Persisted manual provider row missing after restart.");
        JsonNode codex = await Bridge.GetAsync("storage.codexHome", cancellationToken)
            ?? throw new InvalidDataException("Codex Home control missing after restart.");
        JsonNode sqlite = await Bridge.GetAsync("storage.sqliteHome", cancellationToken)
            ?? throw new InvalidDataException("SQLite Home control missing after restart.");
        bool passed = value == 7
            && string.Equals(codex["value"]?.GetValue<string>(), _fixture.CodexHome, StringComparison.OrdinalIgnoreCase)
            && string.Equals(sqlite["value"]?.GetValue<string>(), _fixture.SqliteHome, StringComparison.OrdinalIgnoreCase)
            && string.Equals(providerState["value"]?.GetValue<string>(), "manual-persist-e2e", StringComparison.Ordinal)
            && persistedRow["selected"]?.GetValue<bool>() == true;
        _evidence.Restart["passed"] = passed;
        _evidence.Restart["retention"] = value;
        _evidence.Restart["manualProviderRow"] = providerRow;
        _evidence.Restart["selectedProvider"] = providerState["value"]?.GetValue<string>();
        _evidence.Restart["codexHome"] = codex["value"]?.GetValue<string>();
        _evidence.Restart["sqliteHome"] = sqlite["value"]?.GetValue<string>();
        if (!passed)
        {
            throw new InvalidDataException("GUI settings did not persist across a real EXE restart.");
        }
        PassMany("Retention, storage paths, and a manual provider survived a real published-EXE restart.",
            "gui.settings.recent-home-persistence",
            "gui.settings.sqlite-home-persistence",
            "gui.provider.persist-selection",
            "gui.backup.retention-persistence");
    }

    private async Task CollectAndValidateTraceAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_fixture.TracePath))
        {
            throw new FileNotFoundException("GUI automation trace was not written.", _fixture.TracePath);
        }
        HashSet<string> linkedInTrace = new(StringComparer.Ordinal);
        foreach (string line in await File.ReadAllLinesAsync(_fixture.TracePath, cancellationToken))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            JsonNode trace = JsonNode.Parse(line)
                ?? throw new InvalidDataException("GUI trace contains an empty JSONL record.");
            _evidence.Trace.Add(trace);
            if (trace["eventObserved"]?.GetValue<bool>() != true)
            {
                throw new InvalidDataException($"GUI trace contains a false eventObserved record: {line}");
            }
            if (trace["requestId"]?.GetValue<string>() is { } requestId
                && _linkedRequests.TryGetValue(requestId, out VerifiedApplicationLink? expectedLink))
            {
                VerifiedApplicationLink traced = ApplicationLinkVerifier.Verify(
                    trace,
                    expectedLink.Operation,
                    expectedLink.Lifecycle,
                    expectedLink.ErrorCodes);
                if (!string.Equals(traced.OperationId, expectedLink.OperationId, StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        $"GUI→Application trace operation id mismatch for request {requestId}: "
                        + $"expected {expectedLink.OperationId}, observed {traced.OperationId}.");
                }
                linkedInTrace.Add(requestId);
            }
        }
        string[] missing = _linkedRequests.Keys.Except(linkedInTrace).ToArray();
        if (missing.Length > 0)
        {
            throw new InvalidDataException(
                $"GUI→Application trace linkage is missing for requests: {string.Join(",", missing)}.");
        }
        _evidence.Manifest["applicationTraceLinkedRequests"] = _linkedRequests.Count;
    }

    private async Task ProbeFolderPickerRoundTripAsync(
        string controlId,
        string valueControlId,
        string dialogId,
        string scenarioId,
        string alternatePath,
        string originalPath,
        CancellationToken cancellationToken)
    {
        if (!await ProbeFolderPickerSelectionAsync(
            controlId, dialogId, scenarioId, alternatePath, "alternate", cancellationToken))
        {
            return;
        }
        await RequireStoragePickerResultAsync(valueControlId, alternatePath, cancellationToken);

        if (!await ProbeFolderPickerSelectionAsync(
            controlId, dialogId, scenarioId, originalPath, "restore-original", cancellationToken))
        {
            return;
        }
        await RequireStoragePickerResultAsync(valueControlId, originalPath, cancellationToken);
        Pass(scenarioId,
            "Two real Windows folder pickers selected a different valid isolated path and then restored the original; control and rendered status reflected both choices.");
    }

    private async Task<bool> ProbeFolderPickerSelectionAsync(
        string controlId,
        string dialogId,
        string scenarioId,
        string selectedPath,
        string phase,
        CancellationToken cancellationToken)
    {
        BridgeResponse ready = await Bridge.SendAsync(
            "ui.wait",
            new JsonObject
            {
                ["automationId"] = controlId,
                ["property"] = "enabled",
                ["equals"] = true,
                ["timeoutMs"] = 30000
            },
            cancellationToken);
        if (!ready.Ok)
        {
            string blocker = $"{controlId} did not become actionable before folder picker drive: {ready.ErrorMessage}";
            _evidence.Blockers.Add(blocker);
            Block(scenarioId, blocker);
            return false;
        }
        Task<BridgeResponse> pending = Bridge.SendAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = controlId },
            cancellationToken);
        NativeDialogObservation? dialog = await NativeWindows.WaitForDialogAsync(
            _session!.Process.Id,
            _session.MainWindow,
            TimeSpan.FromSeconds(2),
            cancellationToken);
        if (dialog is null)
        {
            BridgeResponse response = await pending;
            RecordControl(controlId, "ui.invoke", response);
            string blocker = $"App blocker: {controlId} reaches IsolatedAppPlatformBoundary.PickFolder, which returns null without showing the real FolderBrowserDialog.";
            _evidence.Blockers.Add(blocker);
            Block(scenarioId, blocker);
            return false;
        }
        if (IsLocationUnavailable(dialog))
        {
            RecordDialog("dialog.shell.locationUnavailable", controlId, dialog, "acknowledge");
            NativeWindows.ClickDialogButton(dialog.Handle, 1);
            NativeDialogObservation? underlying = await NativeWindows.WaitForDialogAsync(
                _session.Process.Id, _session.MainWindow, TimeSpan.FromSeconds(2), cancellationToken);
            if (underlying is not null)
            {
                NativeWindows.ClickDialogButton(underlying.Handle, 2);
            }
            _ = await pending;
            string blocker = $"Windows shell reported Location unavailable for {controlId}; target picker evidence is rejected.";
            _evidence.Blockers.Add(blocker);
            Block(scenarioId, blocker);
            return false;
        }
        RequirePendingModal(pending, $"{controlId} FolderBrowserDialog");
        RecordDialog(dialogId, $"{controlId}-{phase}", dialog, "accept");
        if (!NativeWindows.TryEnterFolderAndAccept(dialog.Handle, selectedPath))
        {
            NativeWindows.ClickDialogButton(dialog.Handle, 2);
            _ = await pending;
            Block(scenarioId, "Real folder picker was visible, but the external driver could not enter and accept the isolated path.");
            return false;
        }
        BridgeResponse completed = await pending;
        RecordControl(controlId, "ui.invoke", completed);
        if (!completed.Ok)
        {
            string blocker = $"{controlId} rejected the {phase} isolated picker path: {completed.ErrorMessage}";
            _evidence.Blockers.Add(blocker);
            Block(scenarioId, blocker);
            return false;
        }
        _coveredEntries.Add(dialogId);
        BridgeResponse settled = await Bridge.SendAsync(
            "ui.wait",
            new JsonObject
            {
                ["automationId"] = controlId,
                ["property"] = "enabled",
                ["equals"] = true,
                ["timeoutMs"] = 30000
            },
            cancellationToken);
        if (!settled.Ok)
        {
            string blocker = $"{controlId} did not return to enabled state after the {phase} picker selection.";
            _evidence.Blockers.Add(blocker);
            Block(scenarioId, blocker);
            return false;
        }
        return true;
    }

    private async Task RequireStoragePickerResultAsync(
        string valueControlId,
        string expectedPath,
        CancellationToken cancellationToken)
    {
        _ = await RequireStorageControlValueAsync(
            valueControlId, expectedPath, cancellationToken);
        BridgeResponse status = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = "status.output" }, false, cancellationToken);
        RecordControl("status.output", "ui.get", status);
        string renderedStatus = status.Result?["value"]?.GetValue<string>() ?? string.Empty;
        if (!renderedStatus.Contains(Path.GetFullPath(expectedPath), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                $"Real folder picker did not propagate {expectedPath} through the storage control and rendered status.");
        }
    }

    private async Task<BridgeResponse> RequireStorageControlValueAsync(
        string valueControlId,
        string expectedPath,
        CancellationToken cancellationToken)
    {
        BridgeResponse selected = await RequireResponseAsync(
            "ui.get", new JsonObject { ["automationId"] = valueControlId }, false, cancellationToken);
        RecordControl(valueControlId, "ui.get", selected);
        string actualPath = selected.Result?["value"]?.GetValue<string>() ?? string.Empty;
        if (!PathsEqual(actualPath, expectedPath))
        {
            throw new InvalidDataException(
                $"Storage control {valueControlId} did not adopt the real selection {expectedPath}; observed {actualPath}.");
        }
        return selected;
    }

    private async Task<NativeDialogObservation?> InvokeWithDialogAsync(
        string controlId,
        int buttonId,
        string purpose,
        CancellationToken cancellationToken)
    {
        BridgeResponse? response = await InvokeWithDialogResponseAsync(
            controlId, buttonId, purpose, cancellationToken);
        return response is null ? null : _lastDialog;
    }

    private NativeDialogObservation? _lastDialog;

    private async Task<BridgeResponse?> InvokeWithDialogResponseAsync(
        string controlId,
        int buttonId,
        string purpose,
        CancellationToken cancellationToken)
    {
        _lastDialog = null;
        Task<BridgeResponse> pending = Bridge.SendAsync(
            "ui.invoke",
            new JsonObject { ["automationId"] = controlId },
            cancellationToken);
        NativeDialogObservation? dialog = await NativeWindows.WaitForDialogAsync(
            _session!.Process.Id,
            _session.MainWindow,
            TimeSpan.FromSeconds(5),
            cancellationToken);
        if (dialog is null)
        {
            BridgeResponse response = await pending;
            RecordControl(controlId, "ui.invoke", response);
            return null;
        }
        RequirePendingModal(pending, $"{controlId} MessageBox");
        DialogContractResult contract = DialogContractVerifier.Verify(
            purpose,
            dialog.ClassName,
            dialog.Title,
            dialog.Text);
        if (!contract.Passed)
        {
            await RejectUnexpectedDialogAsync(
                pending,
                controlId,
                purpose,
                dialog,
                contract.Error ?? "Unknown native dialog contract failure.",
                cancellationToken);
        }
        _lastDialog = dialog;
        RecordDialog("native.messageBox", purpose, dialog, buttonId == 1 ? "accept" : "cancel");
        NativeWindows.ClickDialogButton(dialog.Handle, buttonId);
        (BridgeResponse completed, bool failureDialog) = await AwaitAfterConfirmationAsync(
            pending,
            controlId,
            purpose,
            dialog.Handle,
            cancellationToken);
        if (failureDialog)
        {
            return null;
        }
        if (!completed.Ok)
        {
            _evidence.Errors.Add($"{controlId} failed after native dialog [{completed.ErrorCode}]: {completed.ErrorMessage}");
            return null;
        }
        return completed;
    }

    private async Task RejectUnexpectedDialogAsync(
        Task<BridgeResponse> pending,
        string controlId,
        string purpose,
        NativeDialogObservation dialog,
        string contractError,
        CancellationToken cancellationToken)
    {
        string screenshotPath = Path.Combine(
            _fixture.Root,
            "automation",
            "screenshots",
            $"{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}-{SafeFileName(purpose)}-invalid-first-dialog.png");
        string? screenshotSha256 = null;
        try
        {
            NativeWindows.CaptureWindow(dialog.Handle, screenshotPath);
            screenshotSha256 = await Hashing.Sha256FileAsync(screenshotPath, cancellationToken);
        }
        catch (Exception screenshotError)
        {
            _evidence.Errors.Add(
                $"Unable to capture invalid {purpose} dialog: {screenshotError.GetType().Name}: {screenshotError.Message}");
            screenshotPath = string.Empty;
        }

        RecordDialog(
            "dialog.operationFailure",
            purpose + "-invalid-first-dialog",
            dialog,
            "acknowledge-and-fail",
            string.IsNullOrEmpty(screenshotPath) ? null : screenshotPath,
            screenshotSha256);
        _coveredEntries.Add("dialog.operationFailure");
        string blocker = $"{controlId} showed a real native dialog that violated the {purpose} contract: {contractError}";
        _evidence.Blockers.Add(blocker);
        NativeWindows.ClickDialogButton(dialog.Handle, 1);
        try
        {
            BridgeResponse completed = await pending.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            RecordControl(controlId, "ui.invoke", completed);
        }
        catch (Exception completionError)
        {
            _evidence.Errors.Add(
                $"Invalid-dialog invoke did not settle after acknowledgement: "
                + $"{completionError.GetType().Name}: {completionError.Message}");
        }
        throw new InvalidDataException(blocker);
    }

    private async Task<(BridgeResponse Response, bool FailureDialog)> AwaitAfterConfirmationAsync(
        Task<BridgeResponse> pending,
        string controlId,
        string purpose,
        nint expectedDialogHandle,
        CancellationToken cancellationToken)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow + _options.Timeout;
        while (!pending.IsCompleted && DateTimeOffset.UtcNow < deadline)
        {
            NativeDialogObservation? unexpected = await NativeWindows.WaitForDialogAsync(
                _session!.Process.Id,
                _session.MainWindow,
                TimeSpan.FromMilliseconds(250),
                cancellationToken,
                expectedDialogHandle);
            if (unexpected is null)
            {
                continue;
            }

            string screenshotPath = Path.Combine(
                _fixture.Root,
                "automation",
                "screenshots",
                $"{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}-{SafeFileName(purpose)}-failure.png");
            NativeWindows.CaptureWindow(unexpected.Handle, screenshotPath);
            string screenshotSha256 = await Hashing.Sha256FileAsync(screenshotPath, cancellationToken);
            RecordDialog(
                "dialog.operationFailure",
                purpose + "-unexpected-failure",
                unexpected,
                "acknowledge",
                screenshotPath,
                screenshotSha256);
            _coveredEntries.Add("dialog.operationFailure");
            string dialogText = string.Join(" | ", new[] { unexpected.Title }.Concat(unexpected.Text));
            string blocker = $"{controlId} showed an unexpected real failure dialog after confirmation: {dialogText}";
            _evidence.Blockers.Add(blocker);
            NativeWindows.ClickDialogButton(unexpected.Handle, 1);
            using CancellationTokenSource responseTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            responseTimeout.CancelAfter(TimeSpan.FromSeconds(10));
            BridgeResponse failed = await pending.WaitAsync(responseTimeout.Token);
            RecordControl(controlId, "ui.invoke", failed);
            return (failed, true);
        }
        if (!pending.IsCompleted)
        {
            throw new TimeoutException($"{controlId} did not complete within {_options.Timeout} after its real confirmation dialog.");
        }
        BridgeResponse completed = await pending;
        RecordControl(controlId, "ui.invoke", completed);
        return (completed, false);
    }

    private static void RequirePendingModal(Task<BridgeResponse> pending, string label)
    {
        if (pending.IsCompleted)
        {
            throw new InvalidDataException(
                $"Named-pipe invoke completed before external desktop driver operated the real {label}; modal evidence would be invalid.");
        }
    }

    private static bool IsLocationUnavailable(NativeDialogObservation dialog)
    {
        string combined = string.Join("\n", new[] { dialog.Title }.Concat(dialog.Text));
        return combined.Contains("位置不可用", StringComparison.OrdinalIgnoreCase)
            || combined.Contains("Location is not available", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<BridgeResponse> RequireResponseAsync(
        string method,
        JsonObject? parameters,
        bool requireApplicationLink,
        CancellationToken cancellationToken)
    {
        BridgeResponse response = await Bridge.SendAsync(method, parameters, cancellationToken);
        if (!response.Ok)
        {
            throw new InvalidOperationException($"{method} failed [{response.ErrorCode}]: {response.ErrorMessage}");
        }
        if (requireApplicationLink)
        {
            string controlId = parameters?["automationId"]?.GetValue<string>() ?? method;
            string expectedOperation = controlId switch
            {
                "status.refresh" => "Status",
                _ => throw new InvalidDataException(
                    $"No explicit Application operation contract is registered for {controlId}.")
            };
            RequireApplicationLink(response, controlId, expectedOperation);
        }
        return response;
    }

    private void RequireApplicationLink(
        BridgeResponse response,
        string controlId,
        string expectedOperation,
        string expectedLifecycle = "Succeeded",
        params string[] expectedErrorCodes)
    {
        if (string.IsNullOrWhiteSpace(response.Id))
        {
            throw new InvalidDataException($"{controlId} completed without a bridge request id.");
        }
        VerifiedApplicationLink link = ApplicationLinkVerifier.Verify(
            response.Result,
            expectedOperation,
            expectedLifecycle,
            expectedErrorCodes);
        if (!_linkedRequests.TryAdd(response.Id, link))
        {
            throw new InvalidDataException($"Duplicate GUI→Application request linkage: {response.Id}.");
        }
    }

    private async Task SetChangedAsync(string automationId, JsonNode value, CancellationToken cancellationToken)
    {
        JsonNode current = await Bridge.GetAsync(automationId, cancellationToken)
            ?? throw new InvalidDataException($"{automationId} is missing.");
        JsonNode? currentValue = current["value"];
        if (JsonNode.DeepEquals(currentValue, value))
        {
            JsonNode alternate = value switch
            {
                JsonValue json when json.TryGetValue<bool>(out bool flag) => JsonValue.Create(!flag)!,
                JsonValue json when json.TryGetValue<int>(out int integer) => JsonValue.Create(integer == 10 ? 9 : 10)!,
                JsonValue json when json.TryGetValue<decimal>(out decimal number) => JsonValue.Create(number == 10 ? 9 : 10)!,
                _ => JsonValue.Create(value.GetValue<string>() + "-changed")!
            };
            BridgeResponse alternateResponse = await RequireResponseAsync(
                "ui.set",
                new JsonObject { ["automationId"] = automationId, ["value"] = alternate },
                false,
                cancellationToken);
            RecordControl(automationId, "ui.set", alternateResponse);
        }
        BridgeResponse response = await RequireResponseAsync(
            "ui.set",
            new JsonObject { ["automationId"] = automationId, ["value"] = value },
            false,
            cancellationToken);
        RecordControl(automationId, "ui.set", response);
    }

    private async Task<string> WaitForDynamicRowAsync(string providerId, CancellationToken cancellationToken)
    {
        string rowId = "provider.row:" + StableInstanceKey(providerId);
        DateTimeOffset deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            BridgeResponse response = await Bridge.SendAsync(
                "ui.get", new JsonObject { ["automationId"] = rowId }, cancellationToken);
            if (response.Ok)
            {
                return rowId;
            }
            await Task.Delay(100, cancellationToken);
        }
        throw new TimeoutException($"Dynamic provider row did not appear: {providerId}.");
    }

    private async Task WaitForDynamicRowMissingAsync(string rowId, CancellationToken cancellationToken)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            BridgeResponse response = await Bridge.SendAsync(
                "ui.get", new JsonObject { ["automationId"] = rowId }, cancellationToken);
            if (!response.Ok && string.Equals(response.ErrorCode, "command-failed", StringComparison.Ordinal))
            {
                return;
            }
            await Task.Delay(100, cancellationToken);
        }
        throw new TimeoutException($"Dynamic provider row did not disappear: {rowId}.");
    }

    private void RecordControl(string automationId, string method, BridgeResponse response) =>
        _evidence.Controls.Add(new()
        {
            ["automationId"] = automationId,
            ["requestId"] = response.Id,
            ["method"] = method,
            ["ok"] = response.Ok,
            ["errorCode"] = response.ErrorCode,
            ["result"] = response.Result?.DeepClone()
        });

    private void RecordDialog(
        string dialogId,
        string purpose,
        NativeDialogObservation dialog,
        string action,
        string? screenshotPath = null,
        string? screenshotSha256 = null) => _evidence.Dialogs.Add(new()
    {
        ["dialogId"] = dialogId,
        ["purpose"] = purpose,
        ["className"] = dialog.ClassName,
        ["title"] = dialog.Title,
        ["text"] = dialog.Text,
        ["action"] = action,
        ["observedAtUtc"] = dialog.ObservedAtUtc,
        ["screenshotPath"] = screenshotPath,
        ["screenshotSha256"] = screenshotSha256
    });

    private static string SafeFileName(string value)
    {
        string safe = new(value.Select(character => char.IsAsciiLetterOrDigit(character) ? character : '-').ToArray());
        return safe.Trim('-');
    }

    private void LoadScenarioContract()
    {
        using JsonDocument document = JsonDocument.Parse(_scenarioJson);
        foreach (string scenario in document.RootElement.GetProperty("nonGatingManifestScenarioIds")
            .EnumerateArray().Select(value => value.GetString()!))
        {
            if (!_nonGatingScenarioIds.Add(scenario))
            {
                throw new InvalidDataException($"Scenario asset duplicates non-gating scenario {scenario}.");
            }
        }
        foreach (JsonElement group in document.RootElement.GetProperty("groups").EnumerateArray())
        {
            string groupId = group.GetProperty("id").GetString()
                ?? throw new InvalidDataException("Scenario group omitted id.");
            string[] evidenceReferences = group.GetProperty("requiredEvidence").EnumerateArray()
                .Select(value => value.GetString()
                    ?? throw new InvalidDataException($"Scenario group {groupId} has a null evidence reference."))
                .ToArray();
            string[] unknownEvidence = evidenceReferences
                .Where(reference => !SupportedEvidenceReferences.Contains(reference))
                .Order(StringComparer.Ordinal)
                .ToArray();
            if (unknownEvidence.Length > 0 || evidenceReferences.Distinct(StringComparer.Ordinal).Count() != evidenceReferences.Length)
            {
                throw new InvalidDataException(
                    $"Scenario group {groupId} has unknown or duplicate evidence references: "
                    + string.Join(",", unknownEvidence));
            }
            string[] entries = group.GetProperty("entryIds").EnumerateArray()
                .Select(value => value.GetString()!).ToArray();
            foreach (string scenario in group.GetProperty("scenarioIds").EnumerateArray()
                .Select(value => value.GetString()!))
            {
                if (!_scenarios.TryAdd(scenario, new ScenarioState("not-run", "No real evidence was recorded.")))
                {
                    throw new InvalidDataException($"Scenario asset duplicates {scenario}.");
                }
                _scenarioEntries[scenario] = entries;
            }
        }
        (_, IReadOnlyList<string> manifestScenarios) = ManifestCoverageGate.ReadRequirements(_manifestJson);
        HashSet<string> partition = _scenarios.Keys.Concat(_nonGatingScenarioIds).ToHashSet(StringComparer.Ordinal);
        if (!manifestScenarios.ToHashSet(StringComparer.Ordinal).SetEquals(partition)
            || _scenarios.Keys.Intersect(_nonGatingScenarioIds, StringComparer.Ordinal).Any())
        {
            throw new InvalidDataException(
                "Scenario asset does not exactly partition required and non-gating manifest scenarioIds.");
        }
    }

    private void FinalizeCoverage()
    {
        if (_coverageFinalized)
        {
            return;
        }
        _coverageFinalized = true;
        foreach ((string id, ScenarioState state) in _scenarios.Where(pair => pair.Value.Status == "not-run").ToArray())
        {
            Block(id, "No isolated real-GUI fault/input path currently proves this manifest scenario.");
        }
        string[] passed = _scenarios.Where(pair => pair.Value.Status == "passed").Select(pair => pair.Key).ToArray();
        (IReadOnlyList<string> manifestEntries, IReadOnlyList<string> manifestScenarios) =
            ManifestCoverageGate.ReadRequirements(_manifestJson);
        ManifestCoverageResult entryCoverage = ManifestCoverageGate.Evaluate(
            _manifestJson, _coveredEntries, manifestScenarios);
        string[] missingRequiredScenarios = _scenarios.Keys.Except(passed, StringComparer.Ordinal)
            .Order(StringComparer.Ordinal).ToArray();
        Dictionary<string, string[]> missingRequiredEvidence = MissingRequiredEvidenceByGroup();
        bool requiredEvidencePassed = missingRequiredEvidence.Count == 0;
        bool coveragePassed = entryCoverage.MissingEntryIds.Count == 0
            && missingRequiredScenarios.Length == 0
            && requiredEvidencePassed;
        _evidence.Manifest["declaredEntryCount"] = manifestEntries.Count;
        _evidence.Manifest["coveredEntryCount"] = manifestEntries.Count - entryCoverage.MissingEntryIds.Count;
        _evidence.Manifest["requiredHeadfulScenarioCount"] = _scenarios.Count;
        _evidence.Manifest["passedRequiredHeadfulScenarioCount"] = passed.Length;
        _evidence.Manifest["nonGatingManifestScenarioCount"] = _nonGatingScenarioIds.Count;
        _evidence.Manifest["nonGatingManifestScenarioIds"] = _nonGatingScenarioIds.Order(StringComparer.Ordinal).ToArray();
        _evidence.Manifest["coveragePassed"] = coveragePassed;
        _evidence.Manifest["missingEntryIds"] = entryCoverage.MissingEntryIds;
        _evidence.Manifest["missingRequiredHeadfulScenarioIds"] = missingRequiredScenarios;
        _evidence.Manifest["requiredEvidencePassed"] = requiredEvidencePassed;
        _evidence.Manifest["missingRequiredEvidenceByGroup"] = missingRequiredEvidence;
        foreach ((string id, ScenarioState state) in _scenarios.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            _evidence.Scenario(id, state.Status, state.Evidence, _scenarioEntries[id]);
        }
        if (!coveragePassed)
        {
            _evidence.Blockers.Add(
                $"Headful gate failed: {entryCoverage.MissingEntryIds.Count} manifest entries, "
                + $"{missingRequiredScenarios.Length} required scenarios, and "
                + $"{missingRequiredEvidence.Sum(pair => pair.Value.Length)} required evidence references lack PASS evidence.");
        }
    }

    private Dictionary<string, string[]> MissingRequiredEvidenceByGroup()
    {
        HashSet<string> available = ResolveAvailableEvidenceReferences();
        Dictionary<string, string[]> missing = new(StringComparer.Ordinal);
        using JsonDocument document = JsonDocument.Parse(_scenarioJson);
        foreach (JsonElement group in document.RootElement.GetProperty("groups").EnumerateArray())
        {
            string groupId = group.GetProperty("id").GetString()
                ?? throw new InvalidDataException("Scenario group omitted id.");
            string[] absent = group.GetProperty("requiredEvidence").EnumerateArray()
                .Select(entry => entry.GetString()
                    ?? throw new InvalidDataException($"Scenario group {groupId} has a null evidence reference."))
                .Where(reference => !available.Contains(reference))
                .Order(StringComparer.Ordinal)
                .ToArray();
            if (absent.Length > 0)
            {
                missing[groupId] = absent;
            }
        }
        _evidence.Manifest["availableEvidenceReferences"] = available.Order(StringComparer.Ordinal).ToArray();
        return missing;
    }

    private HashSet<string> ResolveAvailableEvidenceReferences()
    {
        HashSet<string> available = new(StringComparer.Ordinal);
        if (_evidence.Executable.TryGetValue("sha256", out object? executableHash)
            && executableHash is string hash
            && hash.Length == 64)
        {
            available.Add("published-exe-sha256");
        }
        if (_evidence.Executable.TryGetValue("visibleMainWindow", out object? visible)
            && visible is true)
        {
            available.Add("visible-main-window");
        }
        if (ScenariosPassed("gui.launch", "gui.focus-existing-instance", "gui.shutdown"))
        {
            available.Add("real-window-lifecycle");
        }
        if (_evidence.Controls.Any(control =>
            control.TryGetValue("requestId", out object? requestId)
            && requestId is string id
            && !string.IsNullOrWhiteSpace(id)))
        {
            available.Add("bridge-control-event");
        }
        if (_evidence.Dialogs.Any(dialog =>
            dialog.TryGetValue("dialogId", out object? id)
            && id is string dialogId
            && (dialogId.StartsWith("dialog.storage.", StringComparison.Ordinal)
                || string.Equals(dialogId, "dialog.restore.backupPicker", StringComparison.Ordinal))))
        {
            available.Add("native-folder-dialog");
        }
        if (_evidence.Restart.TryGetValue("passed", out object? restartPassed) && restartPassed is true)
        {
            available.Add("restart-persistence");
        }
        if (_linkedRequests.Count > 0 && _evidence.Trace.Count > 0)
        {
            available.Add("application-trace");
        }
        if (HasRenderedControlText("status.output", text => text.Contains("Codex Home", StringComparison.OrdinalIgnoreCase)))
        {
            available.Add("rendered-status");
        }
        if (ScenarioPassed("gui.status.refresh-failure")
            && _evidence.Dialogs.Any(dialog =>
                string.Equals(dialog.GetValueOrDefault("dialogId") as string, "dialog.operationFailure", StringComparison.Ordinal)))
        {
            available.Add("isolated-failure-path");
        }
        if (_evidence.Dialogs.Any(dialog =>
            string.Equals(dialog.GetValueOrDefault("className") as string, "#32770", StringComparison.Ordinal)))
        {
            available.Add("native-message-box");
        }
        if (_coveredEntries.Contains("provider.row")
            && _coveredEntries.Contains("storage.codexHome.recent"))
        {
            available.Add("stable-dynamic-id");
        }
        if (ScenarioPassed("gui.operation.ready-state")
            && _evidence.Controls.Any(control => control.GetValueOrDefault("enabled") is true))
        {
            available.Add("real-enabled-state");
        }
        if (_evidence.FileDiffs.Any(diff => diff.TryGetValue("phase", out object? phase)
            && phase is string name
            && name is "sync" or "switch" or "restore" or "prune"))
        {
            available.Add("before-after-file-diff");
        }
        if (ScenariosPassed("gui.backup.open-directory", "gui.logs.open-directory"))
        {
            available.Add("no-external-child-process");
        }
        if (ScenariosPassed("gui.update.check", "gui.update.no-update"))
        {
            available.Add("isolated-platform-boundary");
            available.Add("no-network-or-external-process");
        }
        if (HasRenderedControlText("logs.output", text => !string.IsNullOrWhiteSpace(text)))
        {
            available.Add("rendered-log-output");
        }
        if (HasRenderedControlText("warning.closeCodex.primary", text => !string.IsNullOrWhiteSpace(text))
            && HasRenderedControlText("warning.closeCodex.secondary", text => !string.IsNullOrWhiteSpace(text)))
        {
            available.Add("visible-real-control-text");
        }
        return available;
    }

    private bool ScenarioPassed(string id) =>
        _scenarios.TryGetValue(id, out ScenarioState? state)
        && string.Equals(state.Status, "passed", StringComparison.Ordinal);

    private bool ScenariosPassed(params string[] ids) => ids.All(ScenarioPassed);

    private bool HasRenderedControlText(string automationId, Func<string, bool> predicate) =>
        _evidence.Controls
            .Where(control => string.Equals(
                control.GetValueOrDefault("automationId") as string,
                automationId,
                StringComparison.Ordinal))
            .Select(ControlText)
            .Any(text => text is not null && predicate(text));

    private static string? ControlText(Dictionary<string, object?> control)
    {
        if (control.GetValueOrDefault("value") is JsonValue direct
            && direct.TryGetValue(out string? directText))
        {
            return directText;
        }
        if (control.GetValueOrDefault("result") is JsonNode result
            && result["value"] is JsonValue nested
            && nested.TryGetValue(out string? nestedText))
        {
            return nestedText;
        }
        return null;
    }

    private void Pass(string id, string evidence) => _scenarios[id] = new("passed", evidence);
    private void PassMany(string evidence, params string[] ids)
    {
        foreach (string id in ids) Pass(id, evidence);
    }
    private void Block(string id, string evidence)
    {
        if (_scenarios[id].Status != "passed") _scenarios[id] = new("blocked", evidence);
    }
    private void BlockMany(string evidence, params string[] ids)
    {
        foreach (string id in ids) Block(id, evidence);
    }

    private static string StableInstanceKey(string naturalKey)
    {
        string normalized = naturalKey.Trim().Normalize(NormalizationForm.FormKC);
        byte[] hash = System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(normalized));
        return Convert.ToHexString(hash.AsSpan(0, 12)).ToLowerInvariant();
    }

    private static Dictionary<string, object?> Diff(string phase, FixtureSnapshot before, FixtureSnapshot after) => new()
    {
        ["phase"] = phase,
        ["before"] = before,
        ["after"] = after,
        ["configChanged"] = before.ConfigSha256 != after.ConfigSha256,
        ["rolloutChanged"] = before.RolloutSha256 != after.RolloutSha256,
        ["databaseStorageChanged"] = before.DatabaseStorageSha256 != after.DatabaseStorageSha256,
        ["databaseLogicalChanged"] = before.DatabaseProvider != after.DatabaseProvider
            || before.DatabaseModel != after.DatabaseModel,
        ["backupCountDelta"] = after.ManagedBackupCount - before.ManagedBackupCount
    };

    private static bool SameFixtureState(FixtureSnapshot left, FixtureSnapshot right) =>
        left.ConfigSha256 == right.ConfigSha256
        && left.RolloutSha256 == right.RolloutSha256
        && left.DatabaseStorageSha256 == right.DatabaseStorageSha256
        && left.ConfigProvider == right.ConfigProvider
        && left.RolloutProvider == right.RolloutProvider
        && left.DatabaseProvider == right.DatabaseProvider
        && left.DatabaseModel == right.DatabaseModel
        && left.ManagedBackupCount == right.ManagedBackupCount;

    private static bool PathsEqual(string left, string right) =>
        string.Equals(
            Path.TrimEndingDirectorySeparator(Path.GetFullPath(left)),
            Path.TrimEndingDirectorySeparator(Path.GetFullPath(right)),
            StringComparison.OrdinalIgnoreCase);

    private sealed record ScenarioState(string Status, string Evidence);
}
