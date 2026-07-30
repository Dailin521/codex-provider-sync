using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;

namespace CodexProviderSync.Core.Tests;

public sealed class UpdateServiceTests
{
    [Fact]
    public async Task CheckForUpdate_NormalizesAssemblyVersionAndDetectsNewerRelease()
    {
        using HttpClient client = CreateClient(_ => JsonResponse("""
            {
              "tag_name": "v0.3.1",
              "assets": [
                { "name": "CodexProviderSync.exe", "browser_download_url": "https://example.test/CodexProviderSync.exe" },
                { "name": "CodexProviderSync.exe.sha256", "browser_download_url": "https://example.test/CodexProviderSync.exe.sha256" }
              ]
            }
            """));
        UpdateService service = new(client);

        UpdateCheckResult result = await service.CheckForUpdateAsync(new Version(0, 3, 0, 0));

        Assert.True(result.IsUpdateAvailable);
        Assert.Equal(new Version(0, 3, 0), result.CurrentVersion);
        Assert.Equal(new Version(0, 3, 1), result.LatestRelease.Version);
    }

    [Fact]
    public async Task CheckForUpdate_WhenApiIsRateLimited_FallsBackToLatestReleaseRedirect()
    {
        using HttpClient client = CreateClient(request =>
        {
            if (request.RequestUri!.Host == "api.github.com")
            {
                return new HttpResponseMessage(HttpStatusCode.Forbidden);
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = new HttpRequestMessage(
                    HttpMethod.Get,
                    "https://github.com/Dailin521/codex-provider-sync/releases/tag/v0.3.1")
            };
        });
        UpdateService service = new(client);

        UpdateCheckResult result = await service.CheckForUpdateAsync(new Version(0, 3, 0, 0));

        Assert.True(result.IsUpdateAvailable);
        Assert.Equal("v0.3.1", result.LatestRelease.TagName);
        Assert.Equal(
            "https://github.com/Dailin521/codex-provider-sync/releases/download/v0.3.1/CodexProviderSync.exe",
            result.LatestRelease.FindAsset("CodexProviderSync.exe").DownloadUrl.AbsoluteUri);
    }

    [Fact]
    public async Task CheckForUpdate_TimesOutUsingConfiguredDeadline()
    {
        using HttpClient client = CreateClient(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("The cancellation token should stop the request.");
        });
        UpdateService service = new(client, TimeSpan.FromMilliseconds(40));

        TimeoutException error = await Assert.ThrowsAsync<TimeoutException>(
            () => service.CheckForUpdateAsync(new Version(0, 3, 1)));

        Assert.Contains("已取消", error.Message);
        Assert.IsAssignableFrom<OperationCanceledException>(error.InnerException);
    }

    [Fact]
    public async Task CheckForUpdate_ApiFallbackHonorsConfiguredDeadline()
    {
        CancellationToken firstToken = default;
        CancellationToken fallbackToken = default;
        using HttpClient client = CreateClient(async (request, cancellationToken) =>
        {
            if (request.RequestUri!.Host == "api.github.com")
            {
                firstToken = cancellationToken;
                return new HttpResponseMessage(HttpStatusCode.Forbidden);
            }

            fallbackToken = cancellationToken;
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("The configured deadline should stop the fallback request.");
        });
        UpdateService service = new(client, TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAsync<TimeoutException>(
            () => service.CheckForUpdateAsync(new Version(0, 3, 1)));

        Assert.True(firstToken.CanBeCanceled);
        Assert.True(fallbackToken.CanBeCanceled);
    }

    [Fact]
    public async Task CheckForUpdate_ExternalCancellationRemainsCancellation()
    {
        using HttpClient client = CreateClient(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("The cancellation token should stop the request.");
        });
        UpdateService service = new(client, TimeSpan.FromSeconds(10));
        using CancellationTokenSource cancellation = new(TimeSpan.FromMilliseconds(40));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.CheckForUpdateAsync(new Version(0, 3, 1), cancellation.Token));
    }

    [Fact]
    public async Task DownloadWindowsExe_WritesOnlyVerifiedPayload()
    {
        byte[] executable = Encoding.UTF8.GetBytes("verified executable bytes");
        string hash = Convert.ToHexString(SHA256.HashData(executable)).ToLowerInvariant();
        using HttpClient client = CreateClient(request => request.RequestUri!.AbsolutePath switch
        {
            "/CodexProviderSync.exe.sha256" => TextResponse($"{hash}  CodexProviderSync.exe\n"),
            "/CodexProviderSync.exe" => new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(executable) },
            _ => new HttpResponseMessage(HttpStatusCode.NotFound)
        });
        UpdateService service = new(client);
        string directory = Path.Combine(Path.GetTempPath(), $"codex-provider-update-test-{Guid.NewGuid():N}");
        ReleaseInfo release = new("v0.3.1", new Version(0, 3, 1),
        [
            new ReleaseAsset("CodexProviderSync.exe", new Uri("https://example.test/CodexProviderSync.exe")),
            new ReleaseAsset("CodexProviderSync.exe.sha256", new Uri("https://example.test/CodexProviderSync.exe.sha256"))
        ]);

        try
        {
            DownloadedUpdate update = await service.DownloadWindowsExeAsync(release, directory);

            Assert.Equal(executable, await File.ReadAllBytesAsync(update.Path));
            Assert.Equal(hash, update.Sha256);
            Assert.DoesNotContain(Directory.EnumerateFiles(directory), path => path.EndsWith(".download", StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task DownloadWindowsExe_DoesNotUseVersionLookupDeadline()
    {
        byte[] executable = Encoding.UTF8.GetBytes("slow but verified executable");
        string hash = Convert.ToHexString(SHA256.HashData(executable)).ToLowerInvariant();
        using HttpClient client = CreateClient(async (request, cancellationToken) =>
        {
            await Task.Delay(50, cancellationToken);
            return request.RequestUri!.AbsolutePath switch
            {
                "/CodexProviderSync.exe.sha256" => TextResponse($"{hash}  CodexProviderSync.exe\n"),
                "/CodexProviderSync.exe" => new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(executable)
                },
                _ => new HttpResponseMessage(HttpStatusCode.NotFound)
            };
        });
        UpdateService service = new(client, TimeSpan.FromMilliseconds(20));
        string directory = Path.Combine(Path.GetTempPath(), $"codex-provider-update-test-{Guid.NewGuid():N}");
        ReleaseInfo release = new("v0.3.1", new Version(0, 3, 1),
        [
            new ReleaseAsset("CodexProviderSync.exe", new Uri("https://example.test/CodexProviderSync.exe")),
            new ReleaseAsset("CodexProviderSync.exe.sha256", new Uri("https://example.test/CodexProviderSync.exe.sha256"))
        ]);

        try
        {
            DownloadedUpdate update = await service.DownloadWindowsExeAsync(release, directory);

            Assert.Equal(executable, await File.ReadAllBytesAsync(update.Path));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("v0.2.9", 0, 2, 9)]
    [InlineData("v0.3.1", 0, 3, 1)]
    [InlineData(" 1.4.0 ", 1, 4, 0)]
    public void ParseReleaseVersion_AcceptsStableVersionTags(string tag, int major, int minor, int build)
    {
        Assert.Equal(new Version(major, minor, build), UpdateService.ParseReleaseVersion(tag));
    }

    private static HttpClient CreateClient(Func<HttpRequestMessage, HttpResponseMessage> responder) => new(new DelegateHandler(responder));

    private static HttpClient CreateClient(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> responder) =>
        new(new AsyncDelegateHandler(responder));

    private static HttpResponseMessage JsonResponse(string content) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(content, Encoding.UTF8, "application/json")
    };

    private static HttpResponseMessage TextResponse(string content) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(content, Encoding.UTF8, "text/plain")
    };

    private sealed class DelegateHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(responder(request));
    }

    private sealed class AsyncDelegateHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            responder(request, cancellationToken);
    }
}
