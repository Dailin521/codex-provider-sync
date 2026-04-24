namespace CodexProviderSync.App;

static class Program
{
    [STAThread]
    static void Main()
    {
        try
        {
            ApplicationConfiguration.Initialize();
            Application.Run(new MainForm());
        }
        catch (Exception error)
        {
            string logDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "codex-provider-sync");
            Directory.CreateDirectory(logDir);
            string logPath = Path.Combine(logDir, "startup-error.log");
            File.WriteAllText(logPath, error.ToString());
            MessageBox.Show(
                $"Codex Provider Sync failed to start.\n\n{error.Message}\n\nDetails were written to:\n{logPath}",
                "Codex Provider Sync",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }
    }
}
