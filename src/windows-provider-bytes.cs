using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

// Loaded by the existing exclusive PowerShell worker, not a second sync engine.
public static class ProviderByteFile
{
    [Serializable]
    public sealed class ApplyTiming
    {
        public double workerMs;
        public double readHeaderMs;
        public double flushMs;
        public double restoreMtimeMs;
    }

    public sealed class ApplyResult
    {
        public string Result;
        public ApplyTiming Timing;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    struct Info
    {
        public uint Attributes;
        public long CreationTime, AccessTime, WriteTime;
        public uint Volume, SizeHigh, SizeLow, Links, IndexHigh, IndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetFileInformationByHandle(IntPtr handle, out Info info);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool SetFileTime(IntPtr handle, IntPtr creation, IntPtr access, ref long write);

    static bool Matches(Info info, string dev, string ino)
    {
        ulong index = ((ulong)info.IndexHigh << 32) | info.IndexLow;
        return info.Volume.ToString() == dev && index.ToString() == ino && info.Links == 1
            && (info.Attributes & (0x400u | 0x10u)) == 0;
    }

    static Info Inspect(FileStream stream, string dev, string ino, bool requireMatch = true)
    {
        Info info;
        if (!GetFileInformationByHandle(stream.SafeFileHandle.DangerousGetHandle(), out info))
            throw new Win32Exception(Marshal.GetLastWin32Error());
        if (requireMatch && !Matches(info, dev, ino))
            throw new IOException("Rollout identity changed before provider byte access.");
        return info;
    }

    static byte[] Read(FileStream stream, int count, ApplyTiming timing = null)
    {
        var stopwatch = timing == null ? null : Stopwatch.StartNew();
        try
        {
            var bytes = new byte[count];
            stream.Position = 0;
            for (int offset = 0; offset < count;)
            {
                int n = stream.Read(bytes, offset, count - offset);
                if (n == 0) throw new IOException("Rollout header was truncated.");
                offset += n;
            }
            return bytes;
        }
        finally
        {
            if (stopwatch != null) timing.readHeaderMs += stopwatch.Elapsed.TotalMilliseconds;
        }
    }

    static bool Equal(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
        return true;
    }

    static bool Recoverable(byte[] current, byte[] header, byte[] oldBytes, byte[] newBytes, int offset)
    {
        int phase = 0;
        for (int i = 0; i < header.Length; i++)
        {
            int j = i - offset;
            if (j < 0 || j >= oldBytes.Length || oldBytes[j] == newBytes[j])
            {
                if (current[i] != header[i]) return false;
            }
            else if (current[i] == newBytes[j])
            {
                if (phase == 2) return false;
                phase = 1;
            }
            else if (current[i] == oldBytes[j])
            {
                if (phase == 1) phase = 2;
            }
            else return false;
        }
        return true;
    }

    static void Write(FileStream stream, byte[] bytes, int offset, byte[] expected,
                      long size, string dev, string ino, long mtime, ApplyTiming timing)
    {
        stream.Position = offset;
        stream.Write(bytes, 0, bytes.Length);
        var flush = Stopwatch.StartNew();
        try { stream.Flush(true); }
        finally { timing.flushMs += flush.Elapsed.TotalMilliseconds; }
        if (stream.Length < size || !Equal(Read(stream, expected.Length, timing), expected))
            throw new IOException("Provider byte write verification failed.");
        Inspect(stream, dev, ino);
        var restoreMtime = Stopwatch.StartNew();
        try
        {
            if (!SetFileTime(stream.SafeFileHandle.DangerousGetHandle(), IntPtr.Zero, IntPtr.Zero, ref mtime))
                throw new Win32Exception(Marshal.GetLastWin32Error());
        }
        finally { timing.restoreMtimeMs += restoreMtime.Elapsed.TotalMilliseconds; }
        flush.Restart();
        try { stream.Flush(true); }
        finally { timing.flushMs += flush.Elapsed.TotalMilliseconds; }
    }

    public static ApplyResult ApplyWithTiming(FileStream stream, byte[] header, byte[] oldBytes, byte[] newBytes,
                                              int offset, long size, double mtimeMs, string dev, string ino, bool restore)
    {
        var total = Stopwatch.StartNew();
        var timing = new ApplyTiming();
        try
        {
        var before = Inspect(stream, dev, ino, false);
        if (!Matches(before, dev, ino) || stream.Length < Math.Max(size, header.Length))
        {
            if (!restore) return Complete("SKIP_CHANGED", timing, total);
            throw new IOException("Rollout identity changed or file truncated before provider recovery.");
        }
        var current = Read(stream, header.Length, timing);
        var expected = (byte[])header.Clone();
        Array.Copy(newBytes, 0, expected, offset, newBytes.Length);
        // FILETIME and libuv's Unix timestamp have different epochs.
        double currentMs = (before.WriteTime - 116444736000000000L) / 10000.0;
        if (!restore && (stream.Length != size || Math.Abs(currentMs - mtimeMs) > 0.001
                         || !Equal(current, header))) return Complete("SKIP_CHANGED", timing, total);
        if (restore)
        {
            if (!Recoverable(current, header, oldBytes, newBytes, offset))
                throw new IOException("Unknown rollout bytes during provider recovery.");
            if (Equal(current, header) && (stream.Length != size || Math.Abs(currentMs - mtimeMs) <= 0.001))
                return Complete("APPLIED_IN_PLACE", timing, total);
            long restoreTime = stream.Length == size
                ? 116444736000000000L + (long)Math.Round(mtimeMs * 10000.0) : before.WriteTime;
            Write(stream, oldBytes, offset, header, size, dev, ino, restoreTime, timing);
            return Complete("APPLIED_IN_PLACE", timing, total);
        }
        try
        {
            Write(stream, newBytes, offset, expected, size, dev, ino, before.WriteTime, timing);
        }
        catch (Exception failure)
        {
            try
            {
                Inspect(stream, dev, ino);
                if (stream.Length < size || !Recoverable(Read(stream, header.Length, timing), header, oldBytes, newBytes, offset))
                    throw new IOException("Cannot verify bytes for immediate provider recovery.");
                Write(stream, oldBytes, offset, header, size, dev, ino, before.WriteTime, timing);
            }
            catch (Exception recovery)
            {
                throw new AggregateException("Provider write and immediate recovery failed.", failure, recovery);
            }
            throw;
        }
        return Complete("APPLIED_IN_PLACE", timing, total);
        }
        catch (Exception failure)
        {
            timing.workerMs = total.Elapsed.TotalMilliseconds;
            // Diagnostic attachment must never replace the original failure.
            try { failure.Data["providerSyncNativeTiming"] = timing; } catch { }
            throw;
        }
    }

    static ApplyResult Complete(string result, ApplyTiming timing, Stopwatch total)
    {
        timing.workerMs = total.Elapsed.TotalMilliseconds;
        return new ApplyResult { Result = result, Timing = timing };
    }

    // File.Delete avoids PowerShell provider enumeration for the normal temporary-file
    // cleanup path. Callers retain Remove-Item -Force as the readonly/hidden fallback.
    public static bool TryDelete(string path)
    {
        try
        {
            File.Delete(path);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public static string Apply(FileStream stream, byte[] header, byte[] oldBytes, byte[] newBytes,
                               int offset, long size, double mtimeMs, string dev, string ino, bool restore)
    {
        return ApplyWithTiming(stream, header, oldBytes, newBytes, offset, size, mtimeMs, dev, ino, restore).Result;
    }
}
