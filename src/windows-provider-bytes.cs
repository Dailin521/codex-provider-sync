using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;

// Loaded by the existing exclusive PowerShell worker, not a second sync engine.
public static class ProviderByteFile
{
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

    static Info Inspect(FileStream stream, string dev, string ino)
    {
        Info info;
        if (!GetFileInformationByHandle(stream.SafeFileHandle.DangerousGetHandle(), out info))
            throw new Win32Exception(Marshal.GetLastWin32Error());
        ulong index = ((ulong)info.IndexHigh << 32) | info.IndexLow;
        if (info.Volume.ToString() != dev || index.ToString() != ino || info.Links != 1
            || (info.Attributes & (0x400u | 0x10u)) != 0)
            throw new IOException("Rollout identity changed before provider byte access.");
        return info;
    }

    static byte[] Read(FileStream stream, int count)
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
                      long size, string dev, string ino, long mtime)
    {
        stream.Position = offset;
        stream.Write(bytes, 0, bytes.Length);
        stream.Flush(true);
        if (stream.Length < size || !Equal(Read(stream, expected.Length), expected))
            throw new IOException("Provider byte write verification failed.");
        Inspect(stream, dev, ino);
        if (!SetFileTime(stream.SafeFileHandle.DangerousGetHandle(), IntPtr.Zero, IntPtr.Zero, ref mtime))
            throw new Win32Exception(Marshal.GetLastWin32Error());
        stream.Flush(true);
    }

    public static string Apply(FileStream stream, byte[] header, byte[] oldBytes, byte[] newBytes,
                               int offset, long size, double mtimeMs, string dev, string ino, bool restore)
    {
        var before = Inspect(stream, dev, ino);
        if (stream.Length < size) throw new IOException("Rollout truncated before provider byte access.");
        var current = Read(stream, header.Length);
        var expected = (byte[])header.Clone();
        Array.Copy(newBytes, 0, expected, offset, newBytes.Length);
        // FILETIME and libuv's Unix timestamp have different epochs.
        double currentMs = (before.WriteTime - 116444736000000000L) / 10000.0;
        if (!restore && (stream.Length != size || Math.Abs(currentMs - mtimeMs) > 0.001
                         || !Equal(current, header))) return "SKIP_CHANGED";
        if (restore)
        {
            if (!Recoverable(current, header, oldBytes, newBytes, offset))
                throw new IOException("Unknown rollout bytes during provider recovery.");
            if (Equal(current, header)) return "APPLIED_IN_PLACE";
            Write(stream, oldBytes, offset, header, size, dev, ino, before.WriteTime);
            return "APPLIED_IN_PLACE";
        }
        try
        {
            Write(stream, newBytes, offset, expected, size, dev, ino, before.WriteTime);
        }
        catch (Exception failure)
        {
            try
            {
                Inspect(stream, dev, ino);
                if (stream.Length < size || !Recoverable(Read(stream, header.Length), header, oldBytes, newBytes, offset))
                    throw new IOException("Cannot verify bytes for immediate provider recovery.");
                Write(stream, oldBytes, offset, header, size, dev, ino, before.WriteTime);
            }
            catch (Exception recovery)
            {
                throw new AggregateException("Provider write and immediate recovery failed.", failure, recovery);
            }
            throw;
        }
        return "APPLIED_IN_PLACE";
    }
}
