using System.Net.Http.Headers;
using System.Net;
using System.Security.Cryptography;
using System.Text.Json;

namespace CodexProviderSync.Core;

/// <summary>
/// Reads published GitHub releases and downloads a verified Windows GUI update.
/// The caller owns the UI and the process-restart portion of applying an update.
/// </summary>
public sealed class UpdateService
{
    private const string LatestReleaseUrl = "https://api.github.com/repos/Dailin521/codex-provider-sync/releases/latest";
    private const string LatestReleasePageUrl = "https://github.com/Dailin521/codex-provider-sync/releases/latest";
    private const string ReleaseTagPathPrefix = "/Dailin521/codex-provider-sync/releases/tag/";
    private readonly HttpClient _httpClient;

    public UpdateService(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
        if (!_httpClient.DefaultRequestHeaders.UserAgent.Any())
        {
            _httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("CodexProviderSync", "1.0"));
        }
    }

    public async Task<UpdateCheckResult> CheckForUpdateAsync(Version currentVersion, CancellationToken cancellationToken = default)
    {
        using HttpResponseMessage response = await _httpClient.GetAsync(LatestReleaseUrl, cancellationToken);
        if (response.StatusCode is HttpStatusCode.Forbidden or HttpStatusCode.TooManyRequests)
        {
            return await CheckForUpdateViaReleasePageAsync(currentVersion, cancellationToken);
        }

        response.EnsureSuccessStatusCode();

        await using Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using JsonDocument document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        JsonElement root = document.RootElement;
        string tagName = root.GetProperty("tag_name").GetString() ?? throw new InvalidDataException("Release tag is missing.");
        Version version = ParseReleaseVersion(tagName);
        List<ReleaseAsset> assets = [];
        foreach (JsonElement asset in root.GetProperty("assets").EnumerateArray())
        {
            string name = asset.GetProperty("name").GetString() ?? string.Empty;
            string url = asset.GetProperty("browser_download_url").GetString() ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(name) && Uri.TryCreate(url, UriKind.Absolute, out Uri? downloadUrl))
            {
                assets.Add(new ReleaseAsset(name, downloadUrl));
            }
        }

        Version normalizedCurrentVersion = NormalizeVersion(currentVersion);
        ReleaseInfo release = new(tagName, version, assets);
        return new UpdateCheckResult(normalizedCurrentVersion, release, version > normalizedCurrentVersion);
    }

    private async Task<UpdateCheckResult> CheckForUpdateViaReleasePageAsync(Version currentVersion, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _httpClient.GetAsync(
            LatestReleasePageUrl,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();

        Uri finalUri = response.RequestMessage?.RequestUri
            ?? throw new InvalidDataException("GitHub latest release redirect did not provide a destination URL.");
        string path = finalUri.AbsolutePath;
        if (!path.StartsWith(ReleaseTagPathPrefix, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException($"GitHub latest release redirect returned an unexpected URL: {finalUri}");
        }

        string tagName = Uri.UnescapeDataString(path[ReleaseTagPathPrefix.Length..]).Trim('/');
        Version version = ParseReleaseVersion(tagName);
        string assetRoot = $"https://github.com/Dailin521/codex-provider-sync/releases/download/{Uri.EscapeDataString(tagName)}";
        ReleaseInfo release = new(tagName, version,
        [
            new ReleaseAsset("CodexProviderSync.exe", new Uri($"{assetRoot}/CodexProviderSync.exe")),
            new ReleaseAsset("CodexProviderSync.exe.sha256", new Uri($"{assetRoot}/CodexProviderSync.exe.sha256"))
        ]);
        Version normalizedCurrentVersion = NormalizeVersion(currentVersion);
        return new UpdateCheckResult(normalizedCurrentVersion, release, version > normalizedCurrentVersion);
    }

    public async Task<DownloadedUpdate> DownloadWindowsExeAsync(ReleaseInfo release, string downloadDirectory, CancellationToken cancellationToken = default)
    {
        ReleaseAsset exe = release.FindAsset("CodexProviderSync.exe");
        ReleaseAsset checksum = release.FindAsset("CodexProviderSync.exe.sha256");
        Directory.CreateDirectory(downloadDirectory);

        string finalPath = Path.Combine(downloadDirectory, $"CodexProviderSync-{release.Version}.exe");
        string temporaryPath = finalPath + ".download";
        try
        {
            string checksumText = await _httpClient.GetStringAsync(checksum.DownloadUrl, cancellationToken);
            string expectedHash = ParseSha256(checksumText, "CodexProviderSync.exe");

            using HttpResponseMessage response = await _httpClient.GetAsync(exe.DownloadUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            await using (Stream source = await response.Content.ReadAsStreamAsync(cancellationToken))
            await using (FileStream destination = new(temporaryPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true))
            {
                await source.CopyToAsync(destination, cancellationToken);
                await destination.FlushAsync(cancellationToken);
            }

            string actualHash = await CalculateSha256Async(temporaryPath, cancellationToken);
            if (!CryptographicOperations.FixedTimeEquals(Convert.FromHexString(expectedHash), Convert.FromHexString(actualHash)))
            {
                throw new InvalidDataException("The downloaded update does not match the published SHA-256 checksum.");
            }

            File.Move(temporaryPath, finalPath, overwrite: true);
            return new DownloadedUpdate(finalPath, expectedHash);
        }
        catch
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }

            throw;
        }
    }

    internal static Version ParseReleaseVersion(string tagName)
    {
        string value = tagName.Trim();
        if (value.StartsWith("v", StringComparison.OrdinalIgnoreCase))
        {
            value = value[1..];
        }

        if (!Version.TryParse(value, out Version? version))
        {
            throw new InvalidDataException($"Release tag '{tagName}' is not a supported version.");
        }

        return version;
    }

    public static Version NormalizeVersion(Version version) => new(version.Major, version.Minor, Math.Max(version.Build, 0));

    internal static string ParseSha256(string checksumText, string expectedFileName)
    {
        string? line = checksumText
            .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault(value => value.EndsWith(expectedFileName, StringComparison.Ordinal));
        if (line is null)
        {
            throw new InvalidDataException($"Checksum file does not contain {expectedFileName}.");
        }

        string hash = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)[0];
        if (hash.Length != 64 || !hash.All(Uri.IsHexDigit))
        {
            throw new InvalidDataException("Published SHA-256 checksum is invalid.");
        }

        return hash.ToLowerInvariant();
    }

    private static async Task<string> CalculateSha256Async(string path, CancellationToken cancellationToken)
    {
        await using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        byte[] hash = await SHA256.HashDataAsync(stream, cancellationToken);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}

public sealed record ReleaseAsset(string Name, Uri DownloadUrl);

public sealed record ReleaseInfo(string TagName, Version Version, IReadOnlyList<ReleaseAsset> Assets)
{
    public ReleaseAsset FindAsset(string name) => Assets.FirstOrDefault(asset => string.Equals(asset.Name, name, StringComparison.Ordinal))
        ?? throw new InvalidDataException($"Release {TagName} does not contain {name}.");
}

public sealed record UpdateCheckResult(Version CurrentVersion, ReleaseInfo LatestRelease, bool IsUpdateAvailable);

public sealed record DownloadedUpdate(string Path, string Sha256);
