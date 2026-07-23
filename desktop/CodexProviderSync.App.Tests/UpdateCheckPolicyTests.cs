using CodexProviderSync.Core;
using System.Net;
using System.Reflection;
using System.Text;

namespace CodexProviderSync.App.Tests;

public sealed class UpdateCheckPolicyTests
{
    private static readonly DateOnly Today = new(2026, 7, 23);

    [Fact]
    public void AutomaticCheck_RunsOnFirstLaunchAndAgainOnNextLocalDay()
    {
        Assert.True(UpdateCheckPolicy.ShouldRunAutomaticCheck(new AppSettings(), Today));
        Assert.True(
            UpdateCheckPolicy.ShouldRunAutomaticCheck(
                new AppSettings { LastAutomaticUpdateCheckDate = Today.AddDays(-1) },
                Today));
    }

    [Fact]
    public void AutomaticCheck_DoesNotRetryAfterTodaysAttempt()
    {
        AppSettings settings = new() { LastAutomaticUpdateCheckDate = Today };

        Assert.False(UpdateCheckPolicy.ShouldRunAutomaticCheck(settings, Today));
    }

    [Fact]
    public void AutomaticCheck_IsSilentButManualCheckKeepsDialogs()
    {
        Assert.False(UpdateCheckPolicy.ShouldShowNoUpdateDialog(UpdateCheckTrigger.Automatic));
        Assert.False(UpdateCheckPolicy.ShouldShowFailureDialog(UpdateCheckTrigger.Automatic));
        Assert.True(UpdateCheckPolicy.ShouldShowNoUpdateDialog(UpdateCheckTrigger.Manual));
        Assert.True(UpdateCheckPolicy.ShouldShowFailureDialog(UpdateCheckTrigger.Manual));
    }

    [Fact]
    public async Task MainForm_AutomaticCheckPersistsAttemptAndRunsAgainNextDay()
    {
        string root = TempDirectory();
        string settingsPath = Path.Combine(root, "settings.json");
        string logRoot = Path.Combine(root, "logs");
        DateOnly currentDate = Today;
        int requestCount = 0;
        using HttpClient client = new(new DelegateHandler(_ =>
        {
            requestCount += 1;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"tag_name":"v0.3.1","assets":[]}""",
                    Encoding.UTF8,
                    "application/json")
            };
        }));
        SettingsService settingsService = new(settingsPath);
        ExecutionLogService logService = new(logRoot);
        using MainForm form = new(
            logService,
            settingsService,
            new UpdateService(client),
            () => currentDate);

        try
        {
            await InvokeUpdateCheckAsync(form, UpdateCheckTrigger.Automatic);
            await InvokeUpdateCheckAsync(form, UpdateCheckTrigger.Automatic);

            Assert.Equal(1, requestCount);
            Assert.Equal(Today, (await settingsService.LoadAsync()).LastAutomaticUpdateCheckDate);

            currentDate = Today.AddDays(1);
            await InvokeUpdateCheckAsync(form, UpdateCheckTrigger.Automatic);

            Assert.Equal(2, requestCount);
            Assert.Equal(currentDate, (await settingsService.LoadAsync()).LastAutomaticUpdateCheckDate);
            string log = await File.ReadAllTextAsync(logService.CurrentLogPath);
            Assert.Contains("正在自动检查更新", log);
            Assert.Contains("今日已尝试自动检查更新", log);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task MainForm_AutomaticFailureIsLoggedAndStillCountsAsTodaysAttempt()
    {
        string root = TempDirectory();
        string settingsPath = Path.Combine(root, "settings.json");
        string logRoot = Path.Combine(root, "logs");
        using HttpClient client = new(new DelegateHandler(
            _ => throw new HttpRequestException("proxy unavailable")));
        SettingsService settingsService = new(settingsPath);
        ExecutionLogService logService = new(logRoot);
        using MainForm form = new(
            logService,
            settingsService,
            new UpdateService(client),
            () => Today);

        try
        {
            await InvokeUpdateCheckAsync(form, UpdateCheckTrigger.Automatic);

            Assert.Equal(Today, (await settingsService.LoadAsync()).LastAutomaticUpdateCheckDate);
            string log = await File.ReadAllTextAsync(logService.CurrentLogPath);
            Assert.Contains("自动检查更新失败，不影响正常使用", log);
            Assert.Contains("proxy unavailable", log);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static async Task InvokeUpdateCheckAsync(MainForm form, UpdateCheckTrigger trigger)
    {
        MethodInfo method = typeof(MainForm).GetMethod(
            "CheckForUpdatesAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("CheckForUpdatesAsync was not found.");
        Task task = method.Invoke(form, [trigger]) as Task
            ?? throw new InvalidOperationException("CheckForUpdatesAsync did not return a Task.");
        await task;
    }

    private static string TempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"codex-provider-update-policy-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            Task.FromResult(responder(request));
    }
}
