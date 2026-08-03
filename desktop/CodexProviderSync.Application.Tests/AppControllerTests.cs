using CodexProviderSync.Core;

namespace CodexProviderSync.Application.Tests;

public sealed class AppControllerTests
{
    [Fact]
    public async Task InitializeAsync_LoadsStatusAndKeepsThePreferredProvider()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", "/sqlite-override", "relay"),
            RefreshState("relay", supported: true, "openai", "relay"));
        AppController controller = new(core);
        List<AppActivity> activities = [];
        controller.SnapshotChanged += snapshot => activities.Add(snapshot.Activity);

        AppSnapshot snapshot = await controller.InitializeAsync();

        Assert.Equal(AppActivity.Ready, snapshot.Activity);
        Assert.Equal("/codex", snapshot.CodexHome);
        Assert.Equal("/sqlite-override", snapshot.SqliteHomeOverride);
        Assert.Equal("relay", snapshot.SelectedProviderId);
        Assert.True(snapshot.Providers.Single(option => option.Id == "relay").IsSelected);
        Assert.True(snapshot.Controls.RefreshEnabled);
        Assert.True(snapshot.Controls.ExecuteEnabled);
        Assert.Contains(AppActivity.Initializing, activities);
        Assert.Equal("relay", Assert.Single(core.RefreshRequests).SelectedProviderId);

        SyncRequestPreparation preparation = controller.PrepareSyncRequest();
        Assert.True(preparation.IsValid);
        SyncProviderRequest request = Assert.IsType<SyncProviderRequest>(preparation.Request);
        Assert.Equal("relay", request.ProviderId);
    }

    [Fact]
    public async Task ProviderSelection_IsTypedAndMustReferenceAnAvailableOption()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, null),
            RefreshState("openai", supported: true, "openai", "relay"));
        AppController controller = new(core);
        await controller.InitializeAsync();

        AppSnapshot selected = controller.SetProvider("relay");
        Assert.Equal("relay", selected.SelectedProviderId);
        Assert.True(selected.Providers.Single(option => option.Id == "relay").IsSelected);
        Assert.False(selected.Providers.Single(option => option.Id == "openai").IsSelected);

        AppSnapshot cleared = controller.SetProvider(null);
        Assert.True(cleared.HasIssue(AppValidationIssue.ProviderRequired));
        Assert.True(cleared.Controls.ExecuteEnabled);
        Assert.False(controller.PrepareSyncRequest().IsValid);

        Assert.Throws<ArgumentOutOfRangeException>(() => controller.SetProvider("missing"));
    }

    [Fact]
    public async Task ProviderOptions_CanBeRebuiltAfterManualSettingsChange()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "openai"),
            RefreshState("openai", supported: true, "openai"));
        AppController controller = new(core);
        await controller.InitializeAsync();
        ProviderOption manual = new()
        {
            Id = "relay",
            Sources = [ProviderSource.Manual],
            IsManual = true,
            IsSaved = true
        };

        AppSnapshot added = controller.ApplyProviderOptions(
            [.. core.CurrentRefreshState.Providers, manual],
            "relay");
        Assert.Equal("relay", added.SelectedProviderId);
        Assert.True(added.SelectedProvider!.IsManual);

        AppSnapshot removed = controller.ApplyProviderOptions(
            core.CurrentRefreshState.Providers,
            "relay");
        Assert.Equal("openai", removed.SelectedProviderId);
    }

    [Fact]
    public async Task ModelMode_DrivesValidationControlsAndPreparedSwitchRequest()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "relay"),
            RefreshState("openai", supported: true, "openai", "relay"));
        AppController controller = new(core);
        await controller.InitializeAsync();

        AppSnapshot switchEnabled = controller.SetUpdateConfig(true);
        Assert.True(switchEnabled.Controls.ModelModeEnabled);
        Assert.False(switchEnabled.Controls.CustomModelEnabled);
        Assert.IsType<FollowProviderModelSelection>(
            Assert.IsType<SwitchProviderRequest>(controller.PrepareSyncRequest().Request).ModelSelection);

        AppSnapshot custom = controller.SetModelMode(ModelMode.Custom);
        Assert.True(custom.Controls.CustomModelEnabled);
        Assert.True(custom.HasIssue(AppValidationIssue.CustomModelRequired));
        Assert.True(custom.Controls.ExecuteEnabled);

        controller.SetCustomModel("  gpt-custom  ");
        SyncRequestPreparation preparation = controller.PrepareSyncRequest();
        Assert.True(preparation.IsValid);
        SwitchProviderRequest request = Assert.IsType<SwitchProviderRequest>(preparation.Request);
        CustomModelSelection model = Assert.IsType<CustomModelSelection>(request.ModelSelection);
        Assert.Equal("gpt-custom", model.Model);

        controller.SetCustomModel("changed-after-prepare");
        controller.SetProvider(null);
        Assert.Equal("gpt-custom", model.Model);
        Assert.Equal("relay", request.ProviderId);
        controller.SetProvider("relay");

        controller.SetModelMode(ModelMode.KeepRootModel);
        Assert.IsType<KeepRootModelSelection>(
            Assert.IsType<SwitchProviderRequest>(controller.PrepareSyncRequest().Request).ModelSelection);

        AppSnapshot syncOnly = controller.SetUpdateConfig(false);
        Assert.False(syncOnly.Controls.ModelModeEnabled);
        Assert.False(syncOnly.Controls.CustomModelEnabled);
        Assert.IsType<SyncProviderRequest>(controller.PrepareSyncRequest().Request);
    }

    [Fact]
    public async Task Refresh_UsesExplicitPreferredProviderAndStorageForTheNextRequest()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "openai"),
            RefreshState("openai", supported: true, "openai", "relay"));
        AppController controller = new(core);
        await controller.InitializeAsync();

        await controller.RefreshAsync("/other-codex", "/other-sqlite", "relay");
        controller.SetStorage("/edited-codex", "/edited-sqlite");

        Assert.Equal("relay", controller.Snapshot.SelectedProviderId);
        SyncProviderRequest request = Assert.IsType<SyncProviderRequest>(controller.PrepareSyncRequest().Request);
        Assert.Equal("/edited-codex", request.CodexHome);
        Assert.Equal("/edited-sqlite", request.SqliteHomeOverride);
    }

    [Fact]
    public async Task RefreshInProgress_DisablesModelInputsAndRejectsEdits()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "openai"),
            RefreshState("openai", supported: true, "openai"));
        AppController controller = new(core);
        await controller.InitializeAsync();
        controller.SetUpdateConfig(true);

        TaskCompletionSource<CoreRefreshState> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        core.RefreshHandler = (_, _) => pending.Task;
        Task<AppSnapshot> refresh = controller.RefreshAsync("/codex");

        Assert.Equal(AppActivity.Refreshing, controller.Snapshot.Activity);
        Assert.False(controller.Snapshot.Controls.ModelModeEnabled);
        Assert.False(controller.Snapshot.Controls.CustomModelEnabled);
        Assert.Throws<InvalidOperationException>(() => controller.SetModelMode(ModelMode.Custom));

        pending.SetResult(RefreshState("openai", supported: true, "openai"));
        await refresh;
    }

    [Fact]
    public async Task ConcurrentRefresh_IsRejectedWithoutReplacingTheActiveOperation()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "openai"),
            RefreshState("openai", supported: true, "openai"));
        AppController controller = new(core);
        await controller.InitializeAsync();
        TaskCompletionSource<CoreRefreshState> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        core.RefreshHandler = (_, _) => pending.Task;

        Task<AppSnapshot> active = controller.RefreshAsync("/first");
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => controller.RefreshAsync("/second"));
        Assert.Equal("/first", controller.Snapshot.CodexHome);

        pending.SetResult(RefreshState("openai", supported: true, "openai"));
        await active;
    }

    [Fact]
    public async Task Refresh_FallsBackToCurrentProviderAndDisablesUnsupportedSqliteActions()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, "missing"),
            RefreshState("openai", supported: false, "openai", "relay"));
        AppController controller = new(core);

        AppSnapshot snapshot = await controller.InitializeAsync();

        Assert.Equal("openai", snapshot.SelectedProviderId);
        Assert.True(snapshot.HasIssue(AppValidationIssue.SqliteUnsupported));
        Assert.False(snapshot.Controls.ExecuteEnabled);
        Assert.True(snapshot.Controls.RefreshEnabled);
        Assert.False(controller.PrepareSyncRequest().IsValid);
    }

    [Fact]
    public async Task RefreshFailure_LeavesARecoverableFaultedSnapshot()
    {
        FakeCoreAdapter core = new(
            new CoreInitializationState("/codex", null, null),
            RefreshState("openai", supported: true, "openai"));
        AppController controller = new(core);
        await controller.InitializeAsync();
        core.RefreshHandler = (_, _) => throw new InvalidOperationException("refresh failed");

        AppSnapshot snapshot = await controller.RefreshAsync("/other-codex", "/other-sqlite");

        Assert.Equal(AppActivity.Faulted, snapshot.Activity);
        Assert.Equal("/other-codex", snapshot.CodexHome);
        Assert.Equal("/other-sqlite", snapshot.SqliteHomeOverride);
        Assert.Equal("refresh failed", snapshot.ErrorMessage);
        Assert.True(snapshot.HasIssue(AppValidationIssue.RefreshFailed));
        Assert.False(snapshot.Controls.ExecuteEnabled);
        Assert.True(snapshot.Controls.RefreshEnabled);
        Assert.False(controller.PrepareSyncRequest().IsValid);
    }

    private static CoreRefreshState RefreshState(
        string currentProvider,
        bool supported,
        params string[] providerIds)
    {
        StatusSnapshot status = new()
        {
            CodexHome = "/codex",
            SqliteHome = "/codex/sqlite",
            SqliteAccess = supported
                ? SqliteAccessInfo.Direct
                : new SqliteAccessInfo(false, "test-unsupported", "unsupported for test"),
            CurrentProvider = new CurrentProviderInfo(currentProvider, false),
            ConfiguredProviders = providerIds,
            RolloutCounts = new ProviderCounts(),
            LockedRolloutFiles = [],
            UnreadableRolloutFiles = [],
            EncryptedContentCounts = new ProviderCounts(),
            SqliteCounts = null,
            BackupRoot = "/codex/backups_state/provider-sync",
            BackupSummary = new BackupSummary { Count = 0, TotalBytes = 0 }
        };
        IReadOnlyList<ProviderOption> providers = providerIds
            .Select(providerId => new ProviderOption
            {
                Id = providerId,
                Sources = [ProviderSource.Config],
                IsCurrentProvider = string.Equals(providerId, currentProvider, StringComparison.Ordinal)
            })
            .ToArray();
        return new CoreRefreshState(status, providers);
    }

    private sealed class FakeCoreAdapter : ICoreApplicationAdapter
    {
        private readonly CoreInitializationState _initialization;
        private readonly CoreRefreshState _refreshState;

        public FakeCoreAdapter(CoreInitializationState initialization, CoreRefreshState refreshState)
        {
            _initialization = initialization;
            _refreshState = refreshState;
            RefreshHandler = (_, _) => Task.FromResult(_refreshState);
        }

        public List<CoreRefreshRequest> RefreshRequests { get; } = [];

        public CoreRefreshState CurrentRefreshState => _refreshState;

        public Func<CoreRefreshRequest, CancellationToken, Task<CoreRefreshState>> RefreshHandler { get; set; }

        public Task<CoreInitializationState> InitializeAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_initialization);
        }

        public Task<CoreRefreshState> RefreshAsync(
            CoreRefreshRequest request,
            CancellationToken cancellationToken = default)
        {
            RefreshRequests.Add(request);
            return RefreshHandler(request, cancellationToken);
        }
    }
}
