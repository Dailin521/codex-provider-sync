using System.Text.Json;

namespace CodexProviderSync.App;

/// <summary>
/// A path-injected, process-lifetime single-instance lease. The lock file is
/// intentionally persistent: closing then deleting it would introduce an ABA
/// window in which an exiting process could delete a new owner's lock.
/// </summary>
internal sealed class AppInstanceGuard(IAppPathProvider paths)
{
    internal AppInstanceAcquisition Acquire()
    {
        Directory.CreateDirectory(paths.SingletonDirectory);
        string lockPath = Path.Combine(paths.SingletonDirectory, "instance.lock");
        try
        {
            FileStream stream = new(
                lockPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.Read,
                4096,
                FileOptions.WriteThrough);
            AppInstanceOwner owner = new(
                Environment.ProcessId,
                DateTimeOffset.UtcNow,
                Environment.CurrentDirectory);
            stream.SetLength(0);
            JsonSerializer.Serialize(stream, owner, JsonOptions);
            stream.Flush(flushToDisk: true);
            return AppInstanceAcquisition.Owner(stream, owner);
        }
        catch (IOException)
        {
            return AppInstanceAcquisition.NonOwner(ReadOwner(lockPath));
        }
        catch (UnauthorizedAccessException)
        {
            return AppInstanceAcquisition.NonOwner(ReadOwner(lockPath));
        }
    }

    private static AppInstanceOwner? ReadOwner(string lockPath)
    {
        try
        {
            using FileStream stream = new(lockPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return JsonSerializer.Deserialize<AppInstanceOwner>(stream, JsonOptions);
        }
        catch
        {
            return null;
        }
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
}

internal sealed record AppInstanceOwner(
    int ProcessId,
    DateTimeOffset StartedAt,
    string CurrentDirectory);

internal sealed class AppInstanceAcquisition : IDisposable
{
    private FileStream? _stream;

    private AppInstanceAcquisition(bool isOwner, FileStream? stream, AppInstanceOwner? owner)
    {
        IsOwner = isOwner;
        _stream = stream;
        ExistingOwner = owner;
    }

    internal bool IsOwner { get; }
    internal AppInstanceOwner? ExistingOwner { get; }

    internal static AppInstanceAcquisition Owner(FileStream stream, AppInstanceOwner owner) =>
        new(true, stream, owner);

    internal static AppInstanceAcquisition NonOwner(AppInstanceOwner? owner) =>
        new(false, null, owner);

    public void Dispose() => Interlocked.Exchange(ref _stream, null)?.Dispose();
}
