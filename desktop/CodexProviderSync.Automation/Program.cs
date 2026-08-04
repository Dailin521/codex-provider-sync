using System.Text;
using System.Text.Json;

namespace CodexProviderSync.Automation;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.OutputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        using CancellationTokenSource shutdown = new();
        ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            shutdown.Cancel();
        };
        Console.CancelKeyPress += cancelHandler;
        try
        {
            AutomationHost host = new(new AutomationApplicationFactory());
            AutomationRunResult result = await host.RunAsync(args, shutdown.Token);
            if (!string.IsNullOrWhiteSpace(result.Diagnostic))
            {
                await Console.Error.WriteLineAsync(result.Diagnostic);
            }

            string json = JsonSerializer.Serialize(result.Response, AutomationJson.Options);
            await Console.Out.WriteLineAsync(json);
            return result.ExitCode;
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
        }
    }
}
