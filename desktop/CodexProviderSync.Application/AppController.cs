using CodexProviderSync.Core;

namespace CodexProviderSync.Application;

public sealed class AppController
{
    private readonly ICoreApplicationAdapter _core;
    private readonly object _stateGate = new();
    private AppSnapshot _snapshot;

    public AppController(ICoreApplicationAdapter core)
    {
        _core = core ?? throw new ArgumentNullException(nameof(core));
        _snapshot = Recalculate(new AppSnapshot());
    }

    public AppSnapshot Snapshot
    {
        get
        {
            lock (_stateGate)
            {
                return _snapshot;
            }
        }
    }

    public event Action<AppSnapshot>? SnapshotChanged;

    public Task<AppSnapshot> DispatchAsync(
        AppControllerCommand command,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        return command switch
        {
            InitializeAppCommand => InitializeAsync(cancellationToken),
            RefreshStatusCommand refresh => RefreshAsync(
                refresh.CodexHome,
                refresh.SqliteHomeOverride,
                refresh.PreferredProviderId,
                cancellationToken),
            SetStorageCommand storage => Task.FromResult(SetStorage(
                storage.CodexHome,
                storage.SqliteHomeOverride)),
            SelectProviderCommand select => Task.FromResult(SetProvider(select.ProviderId)),
            SetUpdateConfigCommand update => Task.FromResult(SetUpdateConfig(update.Enabled)),
            SetModelModeCommand model => Task.FromResult(SetModelMode(model.Mode)),
            SetCustomModelCommand custom => Task.FromResult(SetCustomModel(custom.Value)),
            _ => throw new ArgumentOutOfRangeException(nameof(command), command, "Unknown controller command.")
        };
    }

    public async Task<AppSnapshot> InitializeAsync(CancellationToken cancellationToken = default)
    {
        AppSnapshot previous = BeginOperation(AppActivity.Initializing);
        try
        {
            CoreInitializationState initial = await _core.InitializeAsync(cancellationToken);
            Publish(Snapshot with
            {
                CodexHome = initial.CodexHome,
                SqliteHomeOverride = NormalizeOptional(initial.SqliteHomeOverride),
                SelectedProviderId = NormalizeOptional(initial.PreferredProviderId)
            });
            return await RefreshCoreAsync(
                initial.CodexHome,
                initial.SqliteHomeOverride,
                initial.PreferredProviderId,
                cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Publish(previous);
            throw;
        }
        catch (Exception error)
        {
            return Publish(Snapshot with
            {
                Activity = AppActivity.Faulted,
                ErrorMessage = error.Message
            });
        }
    }

    public async Task<AppSnapshot> RefreshAsync(
        string codexHome,
        string? sqliteHomeOverride = null,
        string? preferredProviderId = null,
        CancellationToken cancellationToken = default)
    {
        AppSnapshot previous = BeginOperation(
            AppActivity.Refreshing,
            codexHome,
            sqliteHomeOverride);
        try
        {
            return await RefreshCoreAsync(
                codexHome,
                sqliteHomeOverride,
                NormalizeOptional(preferredProviderId) ?? previous.SelectedProviderId,
                cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Publish(previous);
            throw;
        }
        catch (Exception error)
        {
            return Publish(Snapshot with
            {
                Activity = AppActivity.Faulted,
                ErrorMessage = error.Message
            });
        }
    }

    public AppSnapshot SetStorage(string codexHome, string? sqliteHomeOverride = null)
    {
        if (string.IsNullOrWhiteSpace(codexHome))
        {
            throw new ArgumentException("Codex Home is required.", nameof(codexHome));
        }

        return UpdateEditable(snapshot => snapshot with
        {
            CodexHome = codexHome.Trim(),
            SqliteHomeOverride = NormalizeOptional(sqliteHomeOverride)
        });
    }

    public AppSnapshot SetProvider(string? providerId)
    {
        string? normalizedProviderId = NormalizeOptional(providerId);
        return UpdateEditable(snapshot =>
        {
            if (normalizedProviderId is not null
                && !snapshot.Providers.Any(option => string.Equals(option.Id, normalizedProviderId, StringComparison.Ordinal)))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(providerId),
                    providerId,
                    "The provider is not present in the current provider options.");
            }

            return snapshot with { SelectedProviderId = normalizedProviderId };
        });
    }

    public AppSnapshot ApplyProviderOptions(
        IReadOnlyList<ProviderOption> providers,
        string? preferredProviderId = null)
    {
        ArgumentNullException.ThrowIfNull(providers);
        return UpdateEditable(snapshot =>
        {
            string? selectedProviderId = ResolveSelection(
                NormalizeOptional(preferredProviderId) ?? snapshot.SelectedProviderId,
                snapshot.Status?.CurrentProvider.Provider,
                providers);
            IReadOnlyList<ProviderOptionState> providerStates = providers
                .Select(option => new ProviderOptionState(
                    option.Id,
                    Array.AsReadOnly(option.Sources.ToArray()),
                    option.IsCurrentProvider,
                    option.IsManual,
                    option.IsSaved,
                    string.Equals(option.Id, selectedProviderId, StringComparison.Ordinal)))
                .ToList()
                .AsReadOnly();

            return snapshot with
            {
                Providers = providerStates,
                SelectedProviderId = selectedProviderId
            };
        });
    }

    public AppSnapshot SetUpdateConfig(bool enabled)
    {
        return UpdateEditable(snapshot => snapshot with { UpdateConfig = enabled });
    }

    public AppSnapshot SetModelMode(ModelMode mode)
    {
        if (!Enum.IsDefined(mode))
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown model mode.");
        }

        return UpdateEditable(snapshot => snapshot with { ModelMode = mode });
    }

    public AppSnapshot SetCustomModel(string value)
    {
        return UpdateEditable(snapshot => snapshot with { CustomModel = value ?? string.Empty });
    }

    public SyncRequestPreparation PrepareSyncRequest()
    {
        AppSnapshot snapshot = Snapshot;
        AppValidationIssue[] blockingIssues = snapshot.ValidationIssues
            .Where(static issue => issue is
                AppValidationIssue.OperationInProgress or
                AppValidationIssue.RefreshFailed or
                AppValidationIssue.StatusUnavailable or
                AppValidationIssue.ProviderRequired or
                AppValidationIssue.CustomModelRequired or
                AppValidationIssue.SqliteUnsupported)
            .ToArray();
        if (blockingIssues.Length > 0)
        {
            return new SyncRequestPreparation(null, Array.AsReadOnly(blockingIssues));
        }

        string providerId = snapshot.SelectedProviderId!;
        PreparedSyncRequest request;
        if (!snapshot.UpdateConfig)
        {
            request = new SyncProviderRequest(
                snapshot.CodexHome,
                snapshot.SqliteHomeOverride,
                providerId);
        }
        else
        {
            SwitchModelSelection modelSelection = snapshot.ModelMode switch
            {
                ModelMode.FollowProvider => new FollowProviderModelSelection(),
                ModelMode.KeepRootModel => new KeepRootModelSelection(),
                ModelMode.Custom => new CustomModelSelection(snapshot.CustomModel.Trim()),
                _ => throw new InvalidOperationException("Unknown model mode.")
            };
            request = new SwitchProviderRequest(
                snapshot.CodexHome,
                snapshot.SqliteHomeOverride,
                providerId,
                modelSelection);
        }

        return new SyncRequestPreparation(request, []);
    }

    private async Task<AppSnapshot> RefreshCoreAsync(
        string codexHome,
        string? sqliteHomeOverride,
        string? preferredProviderId,
        CancellationToken cancellationToken)
    {
        CoreRefreshState refreshed = await _core.RefreshAsync(
            new CoreRefreshRequest(codexHome, sqliteHomeOverride, preferredProviderId),
            cancellationToken);
        string? selectedProviderId = ResolveSelection(
            preferredProviderId,
            refreshed.Status.CurrentProvider.Provider,
            refreshed.Providers);
        IReadOnlyList<ProviderOptionState> providers = refreshed.Providers
            .Select(option => new ProviderOptionState(
                option.Id,
                Array.AsReadOnly(option.Sources.ToArray()),
                option.IsCurrentProvider,
                option.IsManual,
                option.IsSaved,
                string.Equals(option.Id, selectedProviderId, StringComparison.Ordinal)))
            .ToList()
            .AsReadOnly();

        return Publish(Snapshot with
        {
            Activity = AppActivity.Ready,
            CodexHome = refreshed.Status.CodexHome,
            SqliteHomeOverride = NormalizeOptional(sqliteHomeOverride),
            Status = refreshed.Status,
            Providers = providers,
            SelectedProviderId = selectedProviderId,
            ErrorMessage = null
        });
    }

    private AppSnapshot BeginOperation(
        AppActivity activity,
        string? codexHome = null,
        string? sqliteHomeOverride = null)
    {
        AppSnapshot previous;
        AppSnapshot published;
        lock (_stateGate)
        {
            if (IsBusy(_snapshot.Activity))
            {
                throw new InvalidOperationException("Another controller operation is already in progress.");
            }

            previous = _snapshot;
            published = Recalculate(_snapshot with
            {
                Activity = activity,
                CodexHome = codexHome is null ? _snapshot.CodexHome : codexHome.Trim(),
                SqliteHomeOverride = codexHome is null ? _snapshot.SqliteHomeOverride : NormalizeOptional(sqliteHomeOverride),
                ErrorMessage = null
            });
            _snapshot = published;
        }

        NotifySnapshotChanged(published);
        return previous;
    }

    private AppSnapshot UpdateEditable(Func<AppSnapshot, AppSnapshot> update)
    {
        ArgumentNullException.ThrowIfNull(update);
        AppSnapshot published;
        lock (_stateGate)
        {
            if (IsBusy(_snapshot.Activity))
            {
                throw new InvalidOperationException("Controller state cannot be edited while an operation is in progress.");
            }

            published = Recalculate(update(_snapshot));
            _snapshot = published;
        }

        NotifySnapshotChanged(published);
        return published;
    }

    private AppSnapshot Publish(AppSnapshot snapshot)
    {
        AppSnapshot published;
        lock (_stateGate)
        {
            published = Recalculate(snapshot);
            _snapshot = published;
        }

        NotifySnapshotChanged(published);
        return published;
    }

    private void NotifySnapshotChanged(AppSnapshot snapshot)
    {
        Delegate[] handlers = SnapshotChanged?.GetInvocationList() ?? [];
        foreach (Action<AppSnapshot> handler in handlers.Cast<Action<AppSnapshot>>())
        {
            try
            {
                handler(snapshot);
            }
            catch
            {
                // Observers must not corrupt controller state or leave an
                // operation permanently busy.
            }
        }
    }

    private static AppSnapshot Recalculate(AppSnapshot snapshot)
    {
        string? selectedProviderId = NormalizeOptional(snapshot.SelectedProviderId);
        IReadOnlyList<ProviderOptionState> providers = snapshot.Providers
            .Select(option => option with
            {
                IsSelected = selectedProviderId is not null
                    && string.Equals(option.Id, selectedProviderId, StringComparison.Ordinal)
            })
            .ToList()
            .AsReadOnly();

        bool busy = IsBusy(snapshot.Activity);
        List<AppValidationIssue> issues = [];
        if (busy)
        {
            issues.Add(AppValidationIssue.OperationInProgress);
        }
        if (snapshot.Activity == AppActivity.Faulted)
        {
            issues.Add(AppValidationIssue.RefreshFailed);
        }
        if (snapshot.Status is null)
        {
            issues.Add(AppValidationIssue.StatusUnavailable);
        }
        if (selectedProviderId is null)
        {
            issues.Add(AppValidationIssue.ProviderRequired);
        }
        if (snapshot.UpdateConfig
            && snapshot.ModelMode == ModelMode.Custom
            && string.IsNullOrWhiteSpace(snapshot.CustomModel))
        {
            issues.Add(AppValidationIssue.CustomModelRequired);
        }
        if (snapshot.Status?.SqliteAccess.Supported == false)
        {
            issues.Add(AppValidationIssue.SqliteUnsupported);
        }

        bool ready = snapshot.Activity == AppActivity.Ready && snapshot.Status is not null;
        bool sqliteSupported = snapshot.Status?.SqliteAccess.Supported != false;
        // Keep the existing WinForms behavior: provider/custom-model validation
        // is shown when Execute is invoked rather than disabling the button.
        AppControlAvailability controls = new(
            RefreshEnabled: !busy && !string.IsNullOrWhiteSpace(snapshot.CodexHome),
            ExecuteEnabled: ready && sqliteSupported,
            ProviderSelectionEnabled: ready,
            UpdateConfigEnabled: ready,
            ModelModeEnabled: ready && snapshot.UpdateConfig,
            CustomModelEnabled: ready && snapshot.UpdateConfig && snapshot.ModelMode == ModelMode.Custom);

        return snapshot with
        {
            Providers = providers,
            SelectedProviderId = selectedProviderId,
            ValidationIssues = issues.AsReadOnly(),
            Controls = controls
        };
    }

    private static string? ResolveSelection(
        string? preferredProviderId,
        string? currentProviderId,
        IReadOnlyList<ProviderOption> providers)
    {
        string? preferred = NormalizeOptional(preferredProviderId);
        if (preferred is not null
            && providers.Any(option => string.Equals(option.Id, preferred, StringComparison.Ordinal)))
        {
            return preferred;
        }

        string? current = NormalizeOptional(currentProviderId);
        return current is not null
            && providers.Any(option => string.Equals(option.Id, current, StringComparison.Ordinal))
            ? current
            : null;
    }

    private static bool IsBusy(AppActivity activity)
    {
        return activity is AppActivity.Initializing or AppActivity.Refreshing;
    }

    private static string? NormalizeOptional(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
