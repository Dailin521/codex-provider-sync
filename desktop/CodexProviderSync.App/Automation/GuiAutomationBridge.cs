using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CodexProviderSync.App.Automation;

internal sealed class GuiAutomationBridge : IDisposable
{
    internal const int MaximumMessageBytes = 64 * 1024;
    internal const int MaximumRequestsPerConnection = 1024;
    private static readonly TimeSpan AuthenticationTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan InactivityTimeout = TimeSpan.FromSeconds(30);

    private readonly MainForm _form;
    private readonly string _pipeName;
    private readonly byte[] _token;
    private readonly GuiAutomationDispatcher _dispatcher;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _server;

    internal GuiAutomationBridge(
        MainForm form,
        AutomationBootstrap bootstrap,
        IAppPathProvider paths)
    {
        if (!bootstrap.Enabled || bootstrap.PipeName is null || bootstrap.Token is null)
        {
            throw new ArgumentException("An enabled, validated automation bootstrap is required.", nameof(bootstrap));
        }
        if (!paths.IsAutomation || paths.AutomationTracePath is null)
        {
            throw new ArgumentException("GUI automation requires isolated application paths.", nameof(paths));
        }

        _form = form;
        _pipeName = bootstrap.PipeName;
        _token = Convert.FromHexString(bootstrap.Token);
        _dispatcher = new GuiAutomationDispatcher(form, new GuiAutomationTraceSink(paths.AutomationTracePath));
    }

    internal void Start()
    {
        if (_server is not null)
        {
            throw new InvalidOperationException("GUI automation bridge has already started.");
        }
        GuiAutomationCatalog.ValidateRuntimeCoverage(_form);
        _server = Task.Run(RunSingleClientAsync);
    }

    private async Task RunSingleClientAsync()
    {
        // One server instance and one accepted connection. Once the connection
        // ends (including failed authentication) this descriptor is spent.
        using NamedPipeServerStream server = new(
            _pipeName,
            PipeDirection.InOut,
            1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);
        await server.WaitForConnectionAsync(_shutdown.Token).ConfigureAwait(false);

        bool authenticated = false;
        HashSet<string> requestIds = new(StringComparer.Ordinal);
        while (!_shutdown.IsCancellationRequested && server.IsConnected)
        {
            TimeSpan timeout = authenticated ? InactivityTimeout : AuthenticationTimeout;
            string? line = await ReadBoundedLineAsync(server, MaximumMessageBytes, timeout, _shutdown.Token)
                .ConfigureAwait(false);
            if (line is null)
            {
                return;
            }

            GuiAutomationRequest request;
            try
            {
                request = GuiAutomationRequest.Parse(line);
            }
            catch (Exception error) when (error is JsonException or InvalidDataException)
            {
                await WriteResponseAsync(server, GuiAutomationResponse.Failure(null, "invalid-request", error.Message), _shutdown.Token)
                    .ConfigureAwait(false);
                return;
            }

            if (!authenticated)
            {
                if (!Authenticate(request.Token))
                {
                    await WriteResponseAsync(server, GuiAutomationResponse.Failure(request.Id, "authentication-failed", "Authentication failed."), _shutdown.Token)
                        .ConfigureAwait(false);
                    return;
                }
                authenticated = true;
            }

            if (!requestIds.Add(request.Id))
            {
                await WriteResponseAsync(
                    server,
                    GuiAutomationResponse.Failure(
                        request.Id,
                        "request-replayed",
                        "A GUI automation request id may only be used once."),
                    _shutdown.Token).ConfigureAwait(false);
                return;
            }
            if (requestIds.Count > MaximumRequestsPerConnection)
            {
                await WriteResponseAsync(
                    server,
                    GuiAutomationResponse.Failure(
                        request.Id,
                        "request-limit-exceeded",
                        "The GUI automation connection exceeded its bounded request budget."),
                    _shutdown.Token).ConfigureAwait(false);
                return;
            }

            GuiAutomationResponse response;
            try
            {
                JsonNode? result = await _dispatcher.DispatchAsync(request, _shutdown.Token).ConfigureAwait(false);
                response = GuiAutomationResponse.Success(request.Id, result);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                response = GuiAutomationResponse.Failure(request.Id, "command-failed", error.Message);
            }
            await WriteResponseAsync(server, response, _shutdown.Token).ConfigureAwait(false);

            if (string.Equals(request.Method, "ui.shutdown", StringComparison.Ordinal))
            {
                return;
            }
        }
    }

    private bool Authenticate(string? candidate)
    {
        if (candidate is null || candidate.Length != 64 || !candidate.All(Uri.IsHexDigit))
        {
            return false;
        }
        try
        {
            return CryptographicOperations.FixedTimeEquals(_token, Convert.FromHexString(candidate));
        }
        catch (FormatException)
        {
            return false;
        }
    }

    internal static async Task<string?> ReadBoundedLineAsync(
        Stream stream,
        int maximumBytes,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (maximumBytes < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumBytes));
        }
        using CancellationTokenSource timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        using MemoryStream buffer = new(Math.Min(maximumBytes, 4096));
        byte[] singleByte = new byte[1];
        while (buffer.Length <= maximumBytes)
        {
            int read = await stream.ReadAsync(singleByte, timeoutSource.Token).ConfigureAwait(false);
            if (read == 0)
            {
                return buffer.Length == 0 ? null : DecodeUtf8(buffer);
            }
            if (singleByte[0] == (byte)'\n')
            {
                return DecodeUtf8(buffer);
            }
            if (singleByte[0] != (byte)'\r')
            {
                buffer.WriteByte(singleByte[0]);
            }
        }
        throw new InvalidDataException($"GUI automation message exceeds {maximumBytes} bytes.");
    }

    private static string DecodeUtf8(MemoryStream buffer) =>
        new UTF8Encoding(false, true).GetString(buffer.GetBuffer(), 0, checked((int)buffer.Length));

    private static async Task WriteResponseAsync(
        Stream stream,
        GuiAutomationResponse response,
        CancellationToken cancellationToken)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(response, GuiAutomationJson.Options);
        if (payload.Length > MaximumMessageBytes)
        {
            payload = JsonSerializer.SerializeToUtf8Bytes(
                GuiAutomationResponse.Failure(response.Id, "response-too-large", "Response exceeds the protocol limit."),
                GuiAutomationJson.Options);
        }
        await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(new byte[] { (byte)'\n' }, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _shutdown.Cancel();
        try
        {
            _server?.Wait(TimeSpan.FromSeconds(2));
        }
        catch (AggregateException)
        {
            // Cancellation or a client disconnect is expected during shutdown.
        }
        _shutdown.Dispose();
    }
}

internal sealed record GuiAutomationRequest(
    string Id,
    string Method,
    string? Token,
    JsonElement Parameters)
{
    private static readonly HashSet<string> AllowedMethods = new(StringComparer.Ordinal)
    {
        "ui.describe",
        "ui.snapshot",
        "ui.get",
        "ui.set",
        "ui.invoke",
        "ui.wait",
        "ui.shutdown"
    };

    internal static GuiAutomationRequest Parse(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json, new JsonDocumentOptions
        {
            AllowTrailingCommas = false,
            CommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 16
        });
        JsonElement root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException("Request must be a JSON object.");
        }
        string id = RequiredString(root, "id", 128);
        string method = RequiredString(root, "method", 64);
        if (!AllowedMethods.Contains(method))
        {
            throw new InvalidDataException($"Unsupported GUI automation method: {method}");
        }
        string? token = root.TryGetProperty("token", out JsonElement tokenElement)
            ? tokenElement.GetString()
            : null;
        JsonElement parameters = root.TryGetProperty("params", out JsonElement parameterElement)
            ? parameterElement.Clone()
            : JsonDocument.Parse("{}").RootElement.Clone();
        if (parameters.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException("params must be a JSON object.");
        }
        return new GuiAutomationRequest(id, method, token, parameters);
    }

    internal string RequiredParameter(string name, int maximumLength = 512) =>
        RequiredString(Parameters, name, maximumLength);

    private static string RequiredString(JsonElement root, string name, int maximumLength)
    {
        if (!root.TryGetProperty(name, out JsonElement value)
            || value.ValueKind != JsonValueKind.String
            || string.IsNullOrWhiteSpace(value.GetString())
            || value.GetString()!.Length > maximumLength)
        {
            throw new InvalidDataException($"{name} must be a non-empty string no longer than {maximumLength} characters.");
        }
        return value.GetString()!;
    }
}

internal sealed record GuiAutomationResponse(
    string? Id,
    bool Ok,
    JsonNode? Result,
    string? ErrorCode,
    string? ErrorMessage)
{
    internal static GuiAutomationResponse Success(string id, JsonNode? result) =>
        new(id, true, result, null, null);

    internal static GuiAutomationResponse Failure(string? id, string code, string message) =>
        new(id, false, null, code, message);
}

internal static class GuiAutomationJson
{
    internal static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false
    };
}

internal sealed class GuiAutomationDispatcher
{
    private readonly MainForm _form;
    private readonly GuiAutomationTraceSink _trace;

    internal GuiAutomationDispatcher(MainForm form, GuiAutomationTraceSink trace)
    {
        _form = form;
        _trace = trace;
        GuiAutomationCatalog.ValidateRuntimeCoverage(form);
    }

    internal async Task<JsonNode?> DispatchAsync(GuiAutomationRequest request, CancellationToken cancellationToken)
    {
        return request.Method switch
        {
            "ui.describe" => await OnUiAsync(Describe, cancellationToken).ConfigureAwait(false),
            "ui.snapshot" => await OnUiAsync(Snapshot, cancellationToken).ConfigureAwait(false),
            "ui.get" => await OnUiAsync(() => Get(request.RequiredParameter("automationId")), cancellationToken).ConfigureAwait(false),
            "ui.set" => await OnUiAsync(() => Set(request), cancellationToken).ConfigureAwait(false),
            "ui.invoke" => await OnUiAsync(() => Invoke(request), cancellationToken).ConfigureAwait(false),
            "ui.wait" => await WaitAsync(request, cancellationToken).ConfigureAwait(false),
            "ui.shutdown" => await OnUiAsync(() => Shutdown(request), cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidDataException($"Unsupported method: {request.Method}")
        };
    }

    private JsonNode Describe()
    {
        using Stream stream = typeof(MainForm).Assembly.GetManifestResourceStream(GuiAutomationCatalog.ManifestResourceName)
            ?? throw new InvalidOperationException("Embedded GUI automation manifest was not found.");
        return JsonNode.Parse(stream) ?? throw new InvalidDataException("GUI automation manifest is empty.");
    }

    private JsonNode Snapshot()
    {
        JsonArray controls = [];
        foreach (Control control in GuiAutomationCatalog.EnumerateRuntimeDenominator(_form))
        {
            JsonObject entry = Get(control.Name).AsObject();
            controls.Add(entry);
        }
        return new JsonObject
        {
            ["controls"] = controls
        };
    }

    private JsonNode Get(string automationId)
    {
        Control? control = FindControlOrDefault(automationId);
        if (control is not null)
        {
            return DescribeControl(control);
        }
        if (TryFindProviderRow(automationId, out ListViewItem? providerRow, out _))
        {
            return DescribeProviderRow(providerRow!);
        }
        if (TryFindRecentHome(automationId, out AutomationComboBoxItem? recentHome, out ComboBox? combo))
        {
            return DescribeRecentHome(recentHome!, combo!);
        }

        throw new KeyNotFoundException($"Unknown GUI automation id: {automationId}");
    }

    private JsonNode Set(GuiAutomationRequest request)
    {
        string automationId = request.RequiredParameter("automationId");
        if (TrySelectDynamicEntry(request, automationId, out JsonNode? selected))
        {
            return selected!;
        }
        if (!request.Parameters.TryGetProperty("value", out JsonElement value))
        {
            throw new InvalidDataException("value is required.");
        }
        Control control = FindControl(automationId);
        _form.ValidateAutomationValue(automationId, value);
        string eventName = SetControlValue(control, value, out bool eventObserved);
        _trace.Append(request.Id, request.Method, automationId, eventName, eventObserved);
        return DescribeControl(control);
    }

    private JsonNode Invoke(GuiAutomationRequest request)
    {
        string automationId = request.RequiredParameter("automationId");
        Control control = FindControl(automationId);
        if (control is MainForm window)
        {
            window.RequestBringToFront();
            _trace.Append(request.Id, request.Method, automationId, "Activated", true);
            return DescribeControl(control);
        }
        if (control is not Button button)
        {
            throw new InvalidOperationException($"{automationId} is not an invokable button.");
        }
        if (!button.Enabled || !button.Visible)
        {
            throw new InvalidOperationException($"{automationId} is not currently actionable.");
        }
        bool observed = false;
        EventHandler observer = (_, _) => observed = true;
        button.Click += observer;
        try
        {
            button.PerformClick();
        }
        finally
        {
            button.Click -= observer;
        }
        _trace.Append(request.Id, request.Method, automationId, "Click", observed);
        if (!observed)
        {
            throw new InvalidOperationException($"{automationId} did not raise its real Click event.");
        }
        return DescribeControl(control);
    }

    private async Task<JsonNode?> WaitAsync(GuiAutomationRequest request, CancellationToken cancellationToken)
    {
        string automationId = request.RequiredParameter("automationId");
        string property = request.RequiredParameter("property", 64);
        if (!request.Parameters.TryGetProperty("equals", out JsonElement expected))
        {
            throw new InvalidDataException("equals is required.");
        }
        int timeoutMs = request.Parameters.TryGetProperty("timeoutMs", out JsonElement timeoutElement)
            ? timeoutElement.GetInt32()
            : 5000;
        if (timeoutMs is < 1 or > 30000)
        {
            throw new InvalidDataException("timeoutMs must be between 1 and 30000.");
        }

        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTimeOffset.UtcNow <= deadline)
        {
            JsonNode? actual = await OnUiAsync(
                () => PropertyValue(FindControl(automationId), property),
                cancellationToken).ConfigureAwait(false);
            if (JsonNode.DeepEquals(actual, JsonNode.Parse(expected.GetRawText())))
            {
                return new JsonObject
                {
                    ["matched"] = true,
                    ["value"] = actual?.DeepClone()
                };
            }
            await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException($"Timed out waiting for {automationId}.{property}.");
    }

    private JsonNode Shutdown(GuiAutomationRequest request)
    {
        _trace.Append(request.Id, request.Method, GuiAutomationCatalog.Ids.MainWindow, "Close", true);
        _form.BeginInvoke(_form.Close);
        return new JsonObject { ["accepted"] = true };
    }

    private Control FindControl(string automationId)
    {
        Control? match = FindControlOrDefault(automationId);
        return match ?? throw new KeyNotFoundException($"Unknown GUI automation id: {automationId}");
    }

    private Control? FindControlOrDefault(string automationId) =>
        GuiAutomationCatalog.EnumerateRuntimeDenominator(_form)
            .SingleOrDefault(control => string.Equals(control.Name, automationId, StringComparison.Ordinal));

    private bool TrySelectDynamicEntry(
        GuiAutomationRequest request,
        string automationId,
        out JsonNode? result)
    {
        result = null;
        if (TryFindProviderRow(automationId, out ListViewItem? providerRow, out ListView? providerList))
        {
            RequireSelectionValue(request);
            bool observed = false;
            EventHandler observer = (_, _) => observed = true;
            providerList!.SelectedIndexChanged += observer;
            try
            {
                providerList.SelectedItems.Clear();
                providerRow!.Selected = true;
                providerRow.Focused = true;
                providerRow.EnsureVisible();
            }
            finally
            {
                providerList.SelectedIndexChanged -= observer;
            }
            _trace.Append(request.Id, request.Method, automationId, "SelectedIndexChanged", observed);
            if (!observed || !providerRow!.Selected)
            {
                throw new InvalidOperationException(
                    $"{automationId} did not raise its real ListView selection event.");
            }
            result = DescribeProviderRow(providerRow);
            return true;
        }

        if (TryFindRecentHome(automationId, out AutomationComboBoxItem? recentHome, out ComboBox? combo))
        {
            RequireSelectionValue(request);
            bool observed = false;
            EventHandler observer = (_, _) => observed = true;
            combo!.SelectedIndexChanged += observer;
            try
            {
                combo.SelectedItem = recentHome;
            }
            finally
            {
                combo.SelectedIndexChanged -= observer;
            }
            _trace.Append(request.Id, request.Method, automationId, "SelectedIndexChanged", observed);
            if (!observed || !ReferenceEquals(combo.SelectedItem, recentHome))
            {
                throw new InvalidOperationException(
                    $"{automationId} did not raise its real ComboBox selection event.");
            }
            result = DescribeRecentHome(recentHome!, combo);
            return true;
        }

        return false;
    }

    private static void RequireSelectionValue(GuiAutomationRequest request)
    {
        if (request.Parameters.TryGetProperty("value", out JsonElement value)
            && (value.ValueKind != JsonValueKind.True || !value.GetBoolean()))
        {
            throw new InvalidDataException("Dynamic selection accepts only value=true when a value is supplied.");
        }
    }

    private bool TryFindProviderRow(
        string automationId,
        out ListViewItem? item,
        out ListView? list)
    {
        list = FindControlOrDefault(GuiAutomationCatalog.Ids.ProviderList) as ListView;
        item = list?.Items.Cast<ListViewItem>()
            .SingleOrDefault(candidate => string.Equals(candidate.Name, automationId, StringComparison.Ordinal));
        return item is not null;
    }

    private bool TryFindRecentHome(
        string automationId,
        out AutomationComboBoxItem? item,
        out ComboBox? combo)
    {
        combo = FindControlOrDefault(GuiAutomationCatalog.Ids.CodexHome) as ComboBox;
        item = combo?.Items.OfType<AutomationComboBoxItem>()
            .SingleOrDefault(candidate => string.Equals(
                candidate.AutomationId,
                automationId,
                StringComparison.Ordinal));
        return item is not null;
    }

    private static JsonObject DescribeProviderRow(ListViewItem item) => new()
    {
        ["automationId"] = item.Name,
        ["controlType"] = nameof(ListViewItem),
        ["selected"] = item.Selected,
        ["value"] = item.Text
    };

    private static JsonObject DescribeRecentHome(AutomationComboBoxItem item, ComboBox combo) => new()
    {
        ["automationId"] = item.AutomationId,
        ["controlType"] = "ComboBoxItem",
        ["selected"] = ReferenceEquals(combo.SelectedItem, item),
        ["value"] = item.Value
    };

    private static JsonObject DescribeControl(Control control)
    {
        JsonObject result = new()
        {
            ["automationId"] = control.Name,
            ["controlType"] = control.GetType().Name,
            ["enabled"] = control.Enabled,
            ["visible"] = control.Visible,
            ["value"] = ControlValue(control)
        };
        return result;
    }

    private static JsonNode? ControlValue(Control control) => control switch
    {
        CheckBox checkBox => JsonValue.Create(checkBox.Checked),
        RadioButton radio => JsonValue.Create(radio.Checked),
        NumericUpDown number => JsonValue.Create(number.Value),
        ListView list => new JsonArray(list.Items.Cast<ListViewItem>().Select(item => (JsonNode)new JsonObject
        {
            ["automationId"] = item.Name,
            ["text"] = item.Text,
            ["selected"] = item.Selected
        }).ToArray()),
        _ => JsonValue.Create(control.Text)
    };

    private static JsonNode? PropertyValue(Control control, string property) => property switch
    {
        "value" => ControlValue(control),
        "text" => JsonValue.Create(control.Text),
        "enabled" => JsonValue.Create(control.Enabled),
        "visible" => JsonValue.Create(control.Visible),
        "checked" when control is CheckBox checkBox => JsonValue.Create(checkBox.Checked),
        "checked" when control is RadioButton radio => JsonValue.Create(radio.Checked),
        _ => throw new InvalidDataException($"Unsupported wait property '{property}' for {control.Name}.")
    };

    private static string SetControlValue(Control control, JsonElement value, out bool eventObserved)
    {
        eventObserved = false;
        switch (control)
        {
            case TextBoxBase textBox when !textBox.ReadOnly:
            {
                bool observed = false;
                EventHandler observer = (_, _) => observed = true;
                textBox.TextChanged += observer;
                try { textBox.Text = value.GetString() ?? string.Empty; }
                finally { textBox.TextChanged -= observer; }
                eventObserved = observed;
                return "TextChanged";
            }
            case ComboBox combo:
            {
                bool observed = false;
                EventHandler observer = (_, _) => observed = true;
                combo.TextChanged += observer;
                try { combo.Text = value.GetString() ?? string.Empty; }
                finally { combo.TextChanged -= observer; }
                eventObserved = observed;
                return "TextChanged";
            }
            case CheckBox checkBox:
            {
                bool observed = false;
                EventHandler observer = (_, _) => observed = true;
                checkBox.CheckedChanged += observer;
                try { checkBox.Checked = value.GetBoolean(); }
                finally { checkBox.CheckedChanged -= observer; }
                eventObserved = observed;
                return "CheckedChanged";
            }
            case RadioButton radio:
            {
                bool observed = false;
                EventHandler observer = (_, _) => observed = true;
                radio.CheckedChanged += observer;
                try { radio.Checked = value.GetBoolean(); }
                finally { radio.CheckedChanged -= observer; }
                eventObserved = observed;
                return "CheckedChanged";
            }
            case NumericUpDown number:
            {
                bool observed = false;
                EventHandler observer = (_, _) => observed = true;
                number.ValueChanged += observer;
                try { number.Value = value.GetDecimal(); }
                finally { number.ValueChanged -= observer; }
                eventObserved = observed;
                return "ValueChanged";
            }
            default:
                throw new InvalidOperationException($"{control.Name} is not writable through GUI automation.");
        }
    }

    private async Task<T> OnUiAsync<T>(Func<T> action, CancellationToken cancellationToken)
    {
        if (_form.IsDisposed || _form.Disposing)
        {
            throw new ObjectDisposedException(nameof(MainForm));
        }
        if (!_form.InvokeRequired)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return action();
        }

        return await RunScheduledOnceAsync(
            callback => _form.BeginInvoke(callback),
            action,
            cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<T> RunScheduledOnceAsync<T>(
        Action<Action> schedule,
        Func<T> action,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        ArgumentNullException.ThrowIfNull(action);
        cancellationToken.ThrowIfCancellationRequested();

        TaskCompletionSource<T> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int state = 0; // 0=pending, 1=executing/executed, 2=cancelled before execution
        using CancellationTokenRegistration registration = cancellationToken.Register(
            () =>
            {
                if (Interlocked.CompareExchange(ref state, 2, 0) == 0)
                {
                    completion.TrySetCanceled(cancellationToken);
                }
            });
        try
        {
            schedule(() =>
            {
                if (Interlocked.CompareExchange(ref state, 1, 0) != 0)
                {
                    return;
                }

                try { completion.TrySetResult(action()); }
                catch (Exception error) { completion.TrySetException(error); }
            });
        }
        catch (Exception error)
        {
            if (Interlocked.CompareExchange(ref state, 1, 0) == 0)
            {
                completion.TrySetException(error);
            }
        }
        return await completion.Task.ConfigureAwait(false);
    }
}

internal sealed class GuiAutomationTraceSink
{
    private readonly string _path;
    private readonly object _gate = new();
    private long _sequence;

    internal GuiAutomationTraceSink(string path)
    {
        _path = Path.GetFullPath(path);
    }

    internal void Append(
        string requestId,
        string method,
        string automationId,
        string guiEvent,
        bool eventObserved)
    {
        lock (_gate)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
            JsonObject record = new()
            {
                ["schemaVersion"] = 1,
                ["sequence"] = ++_sequence,
                ["timestampUtc"] = DateTimeOffset.UtcNow,
                ["requestId"] = requestId,
                ["method"] = method,
                ["automationId"] = automationId,
                ["guiEvent"] = guiEvent,
                ["eventObserved"] = eventObserved,
                ["applicationOperationId"] = null,
                ["applicationOperationLinked"] = false
            };
            using FileStream stream = new(_path, FileMode.Append, FileAccess.Write, FileShare.Read);
            using StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true);
            writer.WriteLine(record.ToJsonString(GuiAutomationJson.Options));
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
    }
}
