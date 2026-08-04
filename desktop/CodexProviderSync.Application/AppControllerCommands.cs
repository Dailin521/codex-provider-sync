namespace CodexProviderSync.Application;

public abstract record AppControllerCommand;

public sealed record InitializeAppCommand : AppControllerCommand;

public sealed record RefreshStatusCommand(
    string CodexHome,
    string? SqliteHomeOverride = null,
    string? PreferredProviderId = null)
    : AppControllerCommand;

public sealed record SetStorageCommand(
    string CodexHome,
    string? SqliteHomeOverride = null)
    : AppControllerCommand;

public sealed record SelectProviderCommand(string? ProviderId) : AppControllerCommand;

public sealed record SetUpdateConfigCommand(bool Enabled) : AppControllerCommand;

public sealed record SetModelModeCommand(ModelMode Mode) : AppControllerCommand;

public sealed record SetCustomModelCommand(string Value) : AppControllerCommand;
