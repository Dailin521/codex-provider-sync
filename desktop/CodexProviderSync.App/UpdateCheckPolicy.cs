using CodexProviderSync.Core;

namespace CodexProviderSync.App;

internal enum UpdateCheckTrigger
{
    Automatic,
    Manual
}

internal static class UpdateCheckPolicy
{
    public static bool ShouldRunAutomaticCheck(AppSettings settings, DateOnly today) =>
        settings.LastAutomaticUpdateCheckDate != today;

    public static bool ShouldShowNoUpdateDialog(UpdateCheckTrigger trigger) =>
        trigger == UpdateCheckTrigger.Manual;

    public static bool ShouldShowFailureDialog(UpdateCheckTrigger trigger) =>
        trigger == UpdateCheckTrigger.Manual;
}
