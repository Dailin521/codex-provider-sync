using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Drawing.Imaging;

namespace CodexProviderSync.GuiE2E;

internal sealed record DesktopProbe(bool Passed, string Message, uint ActiveSessionId, int ProcessSessionId, string? DesktopName);
internal sealed record NativeDialogObservation(
    nint Handle,
    string ClassName,
    string Title,
    IReadOnlyList<string> Text,
    DateTimeOffset ObservedAtUtc);

internal static class NativeWindows
{
    private const uint DesktopReadObjects = 0x0001;
    private const uint DesktopSwitchDesktop = 0x0100;
    private const int UoiName = 2;
    private const uint Th32csSnapProcess = 0x00000002;
    private const uint BmClick = 0x00F5;
    private const uint WmSetText = 0x000C;
    private static readonly nint InvalidHandleValue = new(-1);

    internal static DesktopProbe ProbeInteractiveDesktop()
    {
        uint active = WTSGetActiveConsoleSessionId();
        int processSession = Process.GetCurrentProcess().SessionId;
        if (!Environment.UserInteractive)
        {
            return new(false, "Environment.UserInteractive is false.", active, processSession, null);
        }
        if (active == uint.MaxValue || active != (uint)processSession)
        {
            return new(false, $"Process session {processSession} is not active console session {active}.", active, processSession, null);
        }

        nint desktop = OpenInputDesktop(0, false, DesktopReadObjects | DesktopSwitchDesktop);
        if (desktop == 0)
        {
            return new(false, $"OpenInputDesktop failed with Win32 error {Marshal.GetLastWin32Error()}.", active, processSession, null);
        }
        try
        {
            uint needed;
            _ = GetUserObjectInformation(desktop, UoiName, null, 0, out needed);
            StringBuilder name = new((int)Math.Max(needed / 2, 16));
            if (!GetUserObjectInformation(desktop, UoiName, name, (uint)(name.Capacity * 2), out _))
            {
                return new(false, $"Unable to identify input desktop: {Marshal.GetLastWin32Error()}.", active, processSession, null);
            }
            return new(true, "Interactive process is attached to the active input desktop.", active, processSession, name.ToString());
        }
        finally
        {
            _ = CloseDesktop(desktop);
        }
    }

    internal static async Task<nint> WaitForVisibleMainWindowAsync(
        Process process,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            process.Refresh();
            nint handle = process.MainWindowHandle;
            if (handle != 0 && IsWindowVisible(handle))
            {
                return handle;
            }
            if (process.HasExited)
            {
                throw new InvalidOperationException($"Published GUI exited before showing a window (exit {process.ExitCode}).");
            }
            await Task.Delay(100, cancellationToken);
        }
        throw new TimeoutException("Published GUI did not expose a visible main window on the active desktop.");
    }

    internal static async Task<NativeDialogObservation?> WaitForDialogAsync(
        int processId,
        nint mainWindow,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        nint excludedWindow = default)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (nint window in TopLevelWindows(processId))
            {
                if (window != mainWindow && window != excludedWindow && IsWindowVisible(window))
                {
                    return ObserveDialog(window);
                }
            }
            await Task.Delay(50, cancellationToken);
        }
        return null;
    }

    internal static NativeDialogObservation ObserveDialog(nint handle) => new(
        handle,
        GetClass(handle),
        GetText(handle),
        ChildWindows(handle).Select(GetText).Where(static value => !string.IsNullOrWhiteSpace(value)).Distinct().ToArray(),
        DateTimeOffset.UtcNow);

    internal static void CaptureWindow(nint window, string outputPath)
    {
        if (!GetWindowRect(window, out Rect bounds))
        {
            throw new InvalidOperationException($"GetWindowRect failed with Win32 error {Marshal.GetLastWin32Error()}.");
        }
        int width = bounds.Right - bounds.Left;
        int height = bounds.Bottom - bounds.Top;
        if (width < 1 || height < 1)
        {
            throw new InvalidOperationException("Cannot capture an empty native dialog rectangle.");
        }
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        using Bitmap bitmap = new(width, height, PixelFormat.Format32bppArgb);
        using Graphics graphics = Graphics.FromImage(bitmap);
        nint device = graphics.GetHdc();
        bool printed;
        try
        {
            printed = PrintWindow(window, device, 2);
        }
        finally
        {
            graphics.ReleaseHdc(device);
        }
        if (!printed)
        {
            _ = SetForegroundWindow(window);
            Thread.Sleep(50);
            graphics.CopyFromScreen(
                bounds.Left,
                bounds.Top,
                0,
                0,
                new Size(width, height),
                CopyPixelOperation.SourceCopy);
        }
        bitmap.Save(outputPath, ImageFormat.Png);
    }

    internal static void ClickDialogButton(nint dialog, int controlId)
    {
        nint button = GetDlgItem(dialog, controlId);
        if (button == 0)
        {
            string[] preferred = controlId == 2
                ? ["取消", "Cancel", "否", "No"]
                : ["确定", "OK", "是", "Yes"];
            button = ChildWindows(dialog).FirstOrDefault(child =>
                preferred.Any(text => string.Equals(GetText(child).Trim('&'), text, StringComparison.OrdinalIgnoreCase)));
        }
        if (button != 0)
        {
            _ = SendMessage(button, BmClick, 0, 0);
            return;
        }

        _ = SetForegroundWindow(dialog);
        Thread.Sleep(50);
        System.Windows.Forms.SendKeys.SendWait(controlId == 2 ? "{ESC}" : "{ENTER}");
    }

    internal static bool TryEnterFolderAndAccept(nint dialog, string isolatedPath)
    {
        foreach (nint child in Descendants(dialog))
        {
            string className = GetClass(child);
            if (className.Equals("Edit", StringComparison.OrdinalIgnoreCase))
            {
                _ = SendMessageString(child, WmSetText, 0, isolatedPath);
                nint ok = GetDlgItem(dialog, 1);
                if (ok != 0)
                {
                    _ = SendMessage(ok, BmClick, 0, 0);
                    return true;
                }
            }
        }

        if (!SetForegroundWindow(dialog))
        {
            return false;
        }
        try
        {
            System.Windows.Forms.SendKeys.SendWait("^l");
            Thread.Sleep(100);
            System.Windows.Forms.SendKeys.SendWait(EscapeSendKeys(isolatedPath));
            System.Windows.Forms.SendKeys.SendWait("{ENTER}");
            Thread.Sleep(250);
            nint accept = GetDlgItem(dialog, 1);
            if (accept != 0)
            {
                _ = SendMessage(accept, BmClick, 0, 0);
            }
            else
            {
                System.Windows.Forms.SendKeys.SendWait("{ENTER}");
            }
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private static string EscapeSendKeys(string value)
    {
        StringBuilder escaped = new(value.Length * 2);
        foreach (char character in value)
        {
            if (character is '+' or '^' or '%' or '~' or '(' or ')' or '[' or ']' or '{' or '}')
            {
                escaped.Append('{').Append(character).Append('}');
            }
            else
            {
                escaped.Append(character);
            }
        }
        return escaped.ToString();
    }

    internal static IReadOnlySet<int> DescendantProcessIds(int parentProcessId)
    {
        List<(int Pid, int Parent)> rows = [];
        nint snapshot = CreateToolhelp32Snapshot(Th32csSnapProcess, 0);
        if (snapshot == InvalidHandleValue)
        {
            throw new InvalidOperationException($"CreateToolhelp32Snapshot failed: {Marshal.GetLastWin32Error()}.");
        }
        try
        {
            ProcessEntry32 entry = new() { Size = (uint)Marshal.SizeOf<ProcessEntry32>() };
            if (Process32First(snapshot, ref entry))
            {
                do
                {
                    rows.Add(((int)entry.ProcessId, (int)entry.ParentProcessId));
                    entry.Size = (uint)Marshal.SizeOf<ProcessEntry32>();
                }
                while (Process32Next(snapshot, ref entry));
            }
        }
        finally
        {
            _ = CloseHandle(snapshot);
        }

        HashSet<int> descendants = [];
        bool changed;
        do
        {
            changed = false;
            foreach ((int pid, int parent) in rows)
            {
                if ((parent == parentProcessId || descendants.Contains(parent)) && descendants.Add(pid))
                {
                    changed = true;
                }
            }
        }
        while (changed);
        return descendants;
    }

    private static IReadOnlyList<nint> TopLevelWindows(int processId)
    {
        List<nint> result = [];
        _ = EnumWindows((window, state) =>
        {
            _ = GetWindowThreadProcessId(window, out uint owner);
            if (owner == processId)
            {
                result.Add(window);
            }
            return true;
        }, 0);
        return result;
    }

    private static IReadOnlyList<nint> ChildWindows(nint parent)
    {
        List<nint> result = [];
        _ = EnumChildWindows(parent, (window, state) =>
        {
            result.Add(window);
            return true;
        }, 0);
        return result;
    }

    private static IEnumerable<nint> Descendants(nint parent) => ChildWindows(parent);

    private static string GetText(nint window)
    {
        int length = GetWindowTextLength(window);
        StringBuilder text = new(Math.Max(length + 1, 2));
        _ = GetWindowText(window, text, text.Capacity);
        return text.ToString();
    }

    private static string GetClass(nint window)
    {
        StringBuilder name = new(256);
        _ = GetClassName(window, name, name.Capacity);
        return name.ToString();
    }

    private delegate bool EnumWindowsProc(nint window, nint state);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct ProcessEntry32
    {
        public uint Size;
        public uint Usage;
        public uint ProcessId;
        public nint DefaultHeapId;
        public uint ModuleId;
        public uint Threads;
        public uint ParentProcessId;
        public int BasePriority;
        public uint Flags;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)] public string ExeFile;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Rect
    {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [DllImport("kernel32.dll")]
    private static extern uint WTSGetActiveConsoleSessionId();
    [DllImport("user32.dll", SetLastError = true)]
    private static extern nint OpenInputDesktop(uint flags, [MarshalAs(UnmanagedType.Bool)] bool inherit, uint desiredAccess);
    [DllImport("user32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseDesktop(nint desktop);
    [DllImport("user32.dll", EntryPoint = "GetUserObjectInformationW", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetUserObjectInformation(nint handle, int index, StringBuilder? info, uint length, out uint needed);
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool IsWindowVisible(nint window);
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetForegroundWindow(nint window);
    [DllImport("user32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetWindowRect(nint window, out Rect rectangle);
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool PrintWindow(nint window, nint deviceContext, uint flags);
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool EnumWindows(EnumWindowsProc callback, nint state);
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool EnumChildWindows(nint parent, EnumWindowsProc callback, nint state);
    [DllImport("user32.dll")]
    private static extern uint GetWindowThreadProcessId(nint window, out uint processId);
    [DllImport("user32.dll", EntryPoint = "GetWindowTextLengthW")]
    private static extern int GetWindowTextLength(nint window);
    [DllImport("user32.dll", EntryPoint = "GetWindowTextW", CharSet = CharSet.Unicode)]
    private static extern int GetWindowText(nint window, StringBuilder text, int maximum);
    [DllImport("user32.dll", EntryPoint = "GetClassNameW", CharSet = CharSet.Unicode)]
    private static extern int GetClassName(nint window, StringBuilder name, int maximum);
    [DllImport("user32.dll")]
    private static extern nint GetDlgItem(nint dialog, int controlId);
    [DllImport("user32.dll", EntryPoint = "SendMessageW")]
    private static extern nint SendMessage(nint window, uint message, nint wParam, nint lParam);
    [DllImport("user32.dll", EntryPoint = "SendMessageW", CharSet = CharSet.Unicode)]
    private static extern nint SendMessageString(nint window, uint message, nint wParam, string lParam);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nint CreateToolhelp32Snapshot(uint flags, uint processId);
    [DllImport("kernel32.dll", EntryPoint = "Process32FirstW", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool Process32First(nint snapshot, ref ProcessEntry32 entry);
    [DllImport("kernel32.dll", EntryPoint = "Process32NextW", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool Process32Next(nint snapshot, ref ProcessEntry32 entry);
    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseHandle(nint handle);
}
