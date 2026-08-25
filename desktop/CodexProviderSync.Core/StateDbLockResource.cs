using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace CodexProviderSync.Core;

public sealed record StateDbLockResource(
    string Identity,
    string ResourceKey,
    string RealDbParent,
    string StateDbPath,
    string LockPath)
{
    private const string ErrorCodeDataKey = "codex-provider-sync/error-code";
    private const string LockScopeDataKey = "codex-provider-sync/lock-scope";
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint FileShareDelete = 0x00000004;
    private const uint OpenExisting = 3;
    private const uint FileFlagBackupSemantics = 0x02000000;

    public static Task<StateDbLockResource> ResolveAsync(
        string stateDbTargetPath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(stateDbTargetPath))
        {
            throw Unverifiable("The State DB resource path is missing or invalid.");
        }

        string lexicalPath = Path.GetFullPath(stateDbTargetPath);
        if (!string.Equals(
                Path.GetFileName(lexicalPath),
                AppConstants.DbFileBasename,
                StringComparison.OrdinalIgnoreCase))
        {
            throw Unverifiable("The State DB resource filename is not canonical.");
        }

        string lexicalParent = Path.GetDirectoryName(lexicalPath)
            ?? throw Unverifiable("The State DB resource parent cannot be resolved.");
        string realParent;
        try
        {
            realParent = ResolveExistingPhysicalPath(lexicalParent, directory: true);
            string verifiedParent = ResolveExistingPhysicalPath(lexicalParent, directory: true);
            if (!PathEquals(realParent, verifiedParent))
            {
                throw Unverifiable("The State DB physical parent changed while its identity was resolved.");
            }
        }
        catch (UnauthorizedAccessException error)
        {
            throw PermissionDenied("Permission denied while resolving the State DB resource identity.", error);
        }
        catch (InvalidOperationException error) when (IsTyped(error))
        {
            throw;
        }
        catch (Exception error) when (error is IOException or Win32Exception)
        {
            throw Unverifiable("The State DB physical parent identity cannot be verified.", error);
        }

        string physicalFileName = AppConstants.DbFileBasename;
        try
        {
            FileAttributes attributes = File.GetAttributes(lexicalPath);
            if ((attributes & FileAttributes.Directory) != 0)
            {
                throw Unverifiable("The State DB target is not a regular file.");
            }
            string realFile = ResolveExistingPhysicalPath(lexicalPath, directory: false);
            physicalFileName = Path.GetFileName(realFile);
            realParent = Path.GetDirectoryName(realFile)
                ?? throw Unverifiable("The State DB physical parent cannot be resolved.");
            if (!string.Equals(physicalFileName, AppConstants.DbFileBasename, StringComparison.OrdinalIgnoreCase))
            {
                throw Unverifiable("The State DB physical filename is not canonical.");
            }
        }
        catch (FileNotFoundException)
        {
            // A missing database is valid for Restore only after its physical
            // parent has been verified above.
        }
        catch (DirectoryNotFoundException error)
        {
            throw Unverifiable("The State DB physical parent identity cannot be verified.", error);
        }
        catch (UnauthorizedAccessException error)
        {
            throw PermissionDenied("Permission denied while resolving the State DB physical target.", error);
        }
        catch (InvalidOperationException error) when (IsTyped(error))
        {
            throw;
        }
        catch (Exception error) when (error is IOException or Win32Exception)
        {
            throw Unverifiable("The State DB physical target identity cannot be verified.", error);
        }

        string normalizedParent = NormalizeIdentityPart(Path.GetFullPath(realParent));
        string normalizedFileName = NormalizeIdentityPart(physicalFileName);
        string identity = normalizedParent + "\0" + normalizedFileName;
        string resourceKey = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();
        string lockPath = Path.Combine(
            Path.GetFullPath(realParent),
            ".codex-provider-sync",
            "locks",
            resourceKey + ".lock");
        return Task.FromResult(new StateDbLockResource(
            identity,
            resourceKey,
            Path.GetFullPath(realParent),
            Path.Combine(Path.GetFullPath(realParent), physicalFileName),
            lockPath));
    }

    private static string NormalizeIdentityPart(string value) =>
        OperatingSystem.IsWindows() ? value.ToLowerInvariant() : value;

    private static bool PathEquals(string left, string right) => string.Equals(
        Path.GetFullPath(left),
        Path.GetFullPath(right),
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);

    private static string ResolveExistingPhysicalPath(string path, bool directory)
    {
        if (OperatingSystem.IsWindows())
        {
            using SafeFileHandle handle = CreateFileW(
                path,
                0,
                FileShareRead | FileShareWrite | FileShareDelete,
                IntPtr.Zero,
                OpenExisting,
                directory ? FileFlagBackupSemantics : 0,
                IntPtr.Zero);
            if (handle.IsInvalid)
            {
                throw new Win32Exception(Marshal.GetLastPInvokeError());
            }
            StringBuilder buffer = new(32768);
            uint length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 0);
            if (length == 0 || length >= buffer.Capacity)
            {
                throw new Win32Exception(Marshal.GetLastPInvokeError());
            }
            return NormalizeWindowsDevicePath(buffer.ToString());
        }

        IntPtr resolved = realpath(path, IntPtr.Zero);
        if (resolved == IntPtr.Zero)
        {
            throw new Win32Exception(Marshal.GetLastPInvokeError());
        }
        try
        {
            return Marshal.PtrToStringUTF8(resolved)
                ?? throw new IOException("realpath returned an empty path.");
        }
        finally
        {
            free(resolved);
        }
    }

    private static string NormalizeWindowsDevicePath(string path)
    {
        const string uncPrefix = @"\\?\UNC\";
        const string devicePrefix = @"\\?\";
        if (path.StartsWith(uncPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return @"\\" + path[uncPrefix.Length..];
        }
        return path.StartsWith(devicePrefix, StringComparison.OrdinalIgnoreCase)
            ? path[devicePrefix.Length..]
            : path;
    }

    private static InvalidOperationException Unverifiable(string message, Exception? inner = null)
    {
        InvalidOperationException error = new(message, inner);
        error.Data[ErrorCodeDataKey] = LockService.LockUnverifiableErrorCode;
        error.Data[LockScopeDataKey] = "state-db";
        return error;
    }

    private static InvalidOperationException PermissionDenied(string message, Exception inner)
    {
        InvalidOperationException error = new(message, inner);
        error.Data[ErrorCodeDataKey] = "PERMISSION_DENIED";
        error.Data[LockScopeDataKey] = "state-db";
        return error;
    }

    private static bool IsTyped(Exception error) =>
        error.Data.Contains(ErrorCodeDataKey);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        IntPtr securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        IntPtr templateFile);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(
        SafeFileHandle file,
        StringBuilder filePath,
        uint filePathLength,
        uint flags);

    [DllImport("libc", SetLastError = true)]
    private static extern IntPtr realpath(string path, IntPtr resolvedPath);

    [DllImport("libc")]
    private static extern void free(IntPtr pointer);
}
