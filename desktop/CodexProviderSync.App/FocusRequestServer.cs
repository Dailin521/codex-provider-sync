using System;
using System.IO;
using System.IO.Pipes;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;

namespace CodexProviderSync.App;

/// <summary>
/// Cross-process focus broker: the first instance of CodexProviderSync runs
/// this server on a per-user named pipe. Subsequent instances connect, send
/// the "FOCUS" command, and exit; the server invokes the registered handler
/// on the main thread, which is expected to bring the existing window to
/// the foreground.
/// </summary>
public sealed class FocusRequestServer : IDisposable
{
    private const string PipeNamePrefix = "CodexProviderSync.Focus";

    private readonly Action _onFocusRequested;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _loop;
    private bool _disposed;

    public FocusRequestServer(Action onFocusRequested)
    {
        _onFocusRequested = onFocusRequested ?? throw new ArgumentNullException(nameof(onFocusRequested));
    }

    public string PipeName { get; } = BuildPipeName();

    public void Start()
    {
        if (_loop is not null)
        {
            return;
        }
        _loop = Task.Run(RunAsync);
    }

    public async Task<bool> SendFocusRequestAsync(TimeSpan timeout)
    {
        try
        {
            using NamedPipeClientStream client = new(".", PipeName, PipeDirection.Out, PipeOptions.None);
            await client.ConnectAsync(timeout).ConfigureAwait(false);
            using StreamWriter writer = new(client) { AutoFlush = true };
            await writer.WriteLineAsync("FOCUS").ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private async Task RunAsync()
    {
        while (!_shutdown.IsCancellationRequested)
        {
            try
            {
                using NamedPipeServerStream server = new(
                    PipeName,
                    PipeDirection.In,
                    NamedPipeServerStream.MaxAllowedServerInstances,
                    PipeTransmissionMode.Byte,
                    PipeOptions.Asynchronous);
                await server.WaitForConnectionAsync(_shutdown.Token).ConfigureAwait(false);
                using StreamReader reader = new(server);
                string? line = await reader.ReadLineAsync().ConfigureAwait(false);
                if (string.Equals(line, "FOCUS", StringComparison.Ordinal))
                {
                    _onFocusRequested();
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (IOException)
            {
                // Pipe was broken mid-handshake; loop and create a fresh one.
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _shutdown.Cancel();
        try
        {
            _loop?.Wait(TimeSpan.FromSeconds(2));
        }
        catch (AggregateException)
        {
            // The loop is allowed to fault on cancellation; nothing actionable.
        }
        _shutdown.Dispose();
    }

    private static string BuildPipeName()
    {
        // Per-user scope so two users on the same Windows machine do not
        // collide. Identity.Name is "DOMAIN\User" on Windows; the backslash
        // is illegal in pipe names so we swap it out.
        string identity = WindowsIdentity.GetCurrent().Name ?? "default";
        string sanitized = identity.Replace('\\', '_').Replace('/', '_').Replace(' ', '_');
        return $"{PipeNamePrefix}.{sanitized}";
    }
}
