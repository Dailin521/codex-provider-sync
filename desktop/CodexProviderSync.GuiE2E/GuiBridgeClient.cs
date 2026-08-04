using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E;

internal sealed record BridgeResponse(
    string? Id,
    bool Ok,
    JsonNode? Result,
    string? ErrorCode,
    string? ErrorMessage);

internal sealed class GuiBridgeClient : IAsyncDisposable
{
    private readonly NamedPipeClientStream _pipe;
    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;
    private readonly string _token;
    private readonly TimeSpan _requestTimeout;
    private readonly SemaphoreSlim _singleRequest = new(1, 1);
    private int _sequence;

    private GuiBridgeClient(NamedPipeClientStream pipe, string token, TimeSpan requestTimeout)
    {
        _pipe = pipe;
        _token = token;
        _requestTimeout = requestTimeout;
        _reader = new StreamReader(pipe, new UTF8Encoding(false, true), false, 4096, leaveOpen: true);
        _writer = new StreamWriter(pipe, new UTF8Encoding(false), 4096, leaveOpen: true)
        {
            AutoFlush = true,
            NewLine = "\n"
        };
    }

    internal static async Task<GuiBridgeClient> ConnectAsync(
        string pipeName,
        string token,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        NamedPipeClientStream pipe = new(
            ".",
            pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous,
            System.Security.Principal.TokenImpersonationLevel.Identification);
        try
        {
            using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutSource.CancelAfter(timeout);
            await pipe.ConnectAsync(timeoutSource.Token);
            return new GuiBridgeClient(pipe, token, timeout);
        }
        catch
        {
            await pipe.DisposeAsync();
            throw;
        }
    }

    internal async Task<BridgeResponse> SendAsync(
        string method,
        JsonObject? parameters = null,
        CancellationToken cancellationToken = default)
    {
        using CancellationTokenSource requestSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        requestSource.CancelAfter(_requestTimeout);
        bool lockTaken = false;
        try
        {
            await _singleRequest.WaitAsync(requestSource.Token);
            lockTaken = true;
            string id = $"e2e-{Interlocked.Increment(ref _sequence):D4}-{Guid.NewGuid():N}";
            JsonObject request = new()
            {
                ["id"] = id,
                ["method"] = method,
                ["token"] = _token,
                ["params"] = parameters ?? new JsonObject()
            };
            string requestJson = request.ToJsonString(new JsonSerializerOptions(JsonSerializerDefaults.Web));
            if (Encoding.UTF8.GetByteCount(requestJson) > 64 * 1024)
            {
                throw new InvalidDataException("GUI request exceeds the bridge protocol limit.");
            }
            await _writer.WriteLineAsync(requestJson.AsMemory(), requestSource.Token);
            string? responseJson = await _reader.ReadLineAsync(requestSource.Token);
            if (responseJson is null)
            {
                throw new EndOfStreamException("GUI bridge closed before returning a response.");
            }
            BridgeResponse? response = JsonSerializer.Deserialize<BridgeResponse>(
                responseJson,
                new JsonSerializerOptions(JsonSerializerDefaults.Web));
            if (response is null || !string.Equals(response.Id, id, StringComparison.Ordinal))
            {
                throw new InvalidDataException("GUI bridge response id did not match its request.");
            }
            return response;
        }
        catch (OperationCanceledException error) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"GUI bridge request '{method}' exceeded its {_requestTimeout.TotalSeconds:0.###}-second timeout.",
                error);
        }
        finally
        {
            if (lockTaken)
            {
                _singleRequest.Release();
            }
        }
    }

    internal async Task<JsonNode?> RequireAsync(
        string method,
        JsonObject? parameters = null,
        CancellationToken cancellationToken = default)
    {
        BridgeResponse response = await SendAsync(method, parameters, cancellationToken);
        if (!response.Ok)
        {
            throw new InvalidOperationException(
                $"GUI bridge {method} failed [{response.ErrorCode}]: {response.ErrorMessage}");
        }
        return response.Result;
    }

    internal Task<JsonNode?> GetAsync(string automationId, CancellationToken cancellationToken = default) =>
        RequireAsync("ui.get", new JsonObject { ["automationId"] = automationId }, cancellationToken);

    internal Task<JsonNode?> SetAsync(string automationId, JsonNode? value, CancellationToken cancellationToken = default) =>
        RequireAsync("ui.set", new JsonObject { ["automationId"] = automationId, ["value"] = value }, cancellationToken);

    internal Task<JsonNode?> InvokeAsync(string automationId, CancellationToken cancellationToken = default) =>
        RequireAsync("ui.invoke", new JsonObject { ["automationId"] = automationId }, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        _singleRequest.Dispose();
        _reader.Dispose();
        await _writer.DisposeAsync();
        await _pipe.DisposeAsync();
    }
}
