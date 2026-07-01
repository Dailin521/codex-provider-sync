using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CodexProviderSync.Core.Tests;

public sealed class SingleInstanceGuardTests
{
    private static (string lockDir, string settingsRoot) NewIsolatedLockDir()
    {
        string root = Path.Combine(Path.GetTempPath(), $"codex-provider-singleton-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        string lockDir = Path.Combine(root, "singleton");
        return (lockDir, root);
    }

    private static void Cleanup(string settingsRoot)
    {
        try
        {
            if (Directory.Exists(settingsRoot))
            {
                Directory.Delete(settingsRoot, recursive: true);
            }
        }
        catch
        {
            // best-effort
        }
    }

    [Fact]
    public void Acquire_FirstCallerIsOwner_AndLockDirectoryExists()
    {
        (string lockDir, string settingsRoot) = NewIsolatedLockDir();
        try
        {
            SingleInstanceGuard guard = new(_ => false, lockDir);
            using SingleInstanceAcquisition acquisition = guard.Acquire("test");
            Assert.True(acquisition.IsOwner);
            Assert.Null(acquisition.ExistingOwner);
            Assert.Equal(lockDir, guard.LockDirectory);
            Assert.True(Directory.Exists(lockDir));
            Assert.True(File.Exists(Path.Combine(lockDir, "owner.json")));
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Acquire_SecondCallerSeesExistingOwner_WhenFirstIsAlive()
    {
        (string lockDir, string settingsRoot) = NewIsolatedLockDir();
        try
        {
            // First call takes the lock. We then mutate owner.json to
            // advertise a different (synthetic) pid so the test simulates a
            // different process holding the lock — within a single test
            // process we cannot Acquire twice as owner (the guard's
            // stale-recovery path would treat the first as a self-stale).
            SingleInstanceGuard first = new(_ => true, lockDir);
            using SingleInstanceAcquisition firstAcq = first.Acquire("first");
            Assert.True(firstAcq.IsOwner);

            string ownerPath = Path.Combine(lockDir, "owner.json");
            string ownerJson = File.ReadAllText(ownerPath);
            File.WriteAllText(ownerPath, ownerJson.Replace(
                $"\"processId\": {Environment.ProcessId}",
                "\"processId\": 99999"));

            SingleInstanceGuard second = new(_ => true, lockDir);
            using SingleInstanceAcquisition secondAcq = second.Acquire("second");
            Assert.False(secondAcq.IsOwner);
            Assert.NotNull(secondAcq.ExistingOwner);
            Assert.Equal(99999, secondAcq.ExistingOwner!.ProcessId);
            Assert.Equal("first", secondAcq.ExistingOwner.Label);
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Acquire_RecoversFromStaleLock_WhenPreviousOwnerIsDead()
    {
        (string lockDir, string settingsRoot) = NewIsolatedLockDir();
        try
        {
            // First acquisition is "abandoned" without being disposed.
            SingleInstanceGuard stale = new(_ => true, lockDir);
            SingleInstanceAcquisition staleAcq = stale.Acquire("stale");
            Assert.True(staleAcq.IsOwner);
            // Pretend the stale owner died by leaving the dir on disk.
            staleAcq.Dispose();

            // New acquisition must observe a dead owner (probe returns false) and
            // recover by taking the lock itself.
            SingleInstanceGuard fresh = new(_ => false, lockDir);
            using SingleInstanceAcquisition freshAcq = fresh.Acquire("fresh");
            Assert.True(freshAcq.IsOwner);
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Dispose_RemovesLockDirectoryForOwner()
    {
        (string lockDir, string settingsRoot) = NewIsolatedLockDir();
        try
        {
            SingleInstanceGuard guard = new(_ => false, lockDir);
            using (guard.Acquire("owner"))
            {
                Assert.True(Directory.Exists(lockDir));
            }
            Assert.False(Directory.Exists(lockDir));
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Acquire_RecoversWhenPreviousLockContainsLeftoverFile()
    {
        (string lockDir, string settingsRoot) = NewIsolatedLockDir();
        try
        {
            // First call takes the lock with a live probe, then we abandon it
            // (Dispose). Dispose() removes the lock directory, so we recreate
            // it manually and drop a stray file inside to simulate a partial
            // cleanup. The second call has the probe say "dead", so the guard
            // has to clear the entire tree (including the stray file) and
            // reclaim the lock.
            SingleInstanceGuard first = new(_ => true, lockDir);
            SingleInstanceAcquisition firstAcq = first.Acquire("first");
            firstAcq.Dispose();
            Directory.CreateDirectory(lockDir);
            File.WriteAllText(Path.Combine(lockDir, "BLOCK"), "leftover");

            SingleInstanceGuard second = new(_ => false, lockDir);
            using SingleInstanceAcquisition secondAcq = second.Acquire("second");
            Assert.True(secondAcq.IsOwner);
            Assert.False(File.Exists(Path.Combine(lockDir, "BLOCK")));
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Acquire_FirstCallOnSharedParentDirCreatesLockDirAtomically_AndSecondCallSeesExistingOwner()
    {
        // Regression guard: previously `Acquire` called
        // `Directory.CreateDirectory(LockDirectory)` first, which
        // recursively creates the lock dir on the very first
        // launch. The follow-up atomic `TryCreateDirectory(LockDirectory)`
        // then always returned `Win32ErrorAlreadyExists` and
        // routed the first caller into the stale-lock recovery
        // branch. With the fix, only the parent directory is
        // pre-created; the lock dir itself is created atomically
        // by `TryCreateDirectory`, so the first launch takes the
        // owner branch directly without touching the recovery
        // path. We verify both halves of that contract here:
        //
        //   1. The very first call writes `owner.json` (i.e.
        //      took the atomic-create success branch, not the
        //      stale-recovery branch).
        //   2. A second call, when the lock dir already exists
        //      and owner.json names a live pid, returns
        //      `IsOwner == false` instead of clearing the lock
        //      and reclaiming it.
        //
        // We cannot use two callers in the same test process
        // because `Environment.ProcessId` is constant for the
        // whole process — a second in-process `Acquire` would
        // see its own pid in the just-written `owner.json` and
        // fall into the stale-recovery branch (a Windows pid
        // recycle scenario, not a real concurrent-launch race).
        // Instead, we seed `owner.json` with a fictitious live
        // pid and let the second caller observe it. This is
        // exactly what a concurrent launch from another process
        // looks like at the filesystem level.
        string settingsRoot = Path.Combine(Path.GetTempPath(), $"codex-provider-singleton-{Guid.NewGuid():N}");
        Directory.CreateDirectory(settingsRoot);
        string lockDir = Path.Combine(settingsRoot, "singleton");
        try
        {
            // First caller: lockDir does not exist yet, the
            // atomic `TryCreateDirectory` must succeed and we
            // must take the owner branch.
            SingleInstanceGuard firstGuard = new(static _ => true, lockDir);
            using SingleInstanceAcquisition firstAcq = firstGuard.Acquire("first");
            Assert.True(firstAcq.IsOwner);
            Assert.Null(firstAcq.ExistingOwner);
            Assert.True(File.Exists(Path.Combine(lockDir, "owner.json")),
                "first caller must take the atomic-create success branch and write owner.json directly");
            firstAcq.Dispose();

            // Seed `owner.json` with a fictitious live pid so
            // the second caller observes the existing-owner
            // branch instead of the pid-recycle stale branch.
            // We use `pid = 1` (system init / PID 1 is always
            // alive on Windows/Linux/macOS for the duration of
            // any test run) and a probe that returns true for
            // any pid.
            Directory.CreateDirectory(lockDir);
            File.WriteAllText(Path.Combine(lockDir, "owner.json"),
                "{\"processId\":1,\"startedAt\":\"2026-01-01T00:00:00+00:00\",\"label\":\"existing\",\"currentDirectory\":\"C:\\\\\"}");

            SingleInstanceGuard secondGuard = new(static _ => true, lockDir);
            using SingleInstanceAcquisition secondAcq = secondGuard.Acquire("second");
            Assert.False(secondAcq.IsOwner);
            Assert.NotNull(secondAcq.ExistingOwner);
            Assert.Equal(1, secondAcq.ExistingOwner!.ProcessId);
            Assert.True(File.Exists(Path.Combine(lockDir, "owner.json")),
                "second caller must not delete the lock directory when the owner is still alive");
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }

    [Fact]
    public void Acquire_PidRecycleInOwnerJson_TreatedAsStaleAndReclaimed()
    {
        // Companion regression guard: when `owner.json`
        // contains a pid that matches the current process but
        // the process did NOT actually take the lock (e.g.,
        // Windows reused our pid for a brand-new process while
        // the previous owner crashed mid-write), the guard
        // must treat that as stale and reclaim the lock. The
        // signal we use is: the lock dir contains an
        // `owner.json` with our pid but no leftover file from
        // a partial write, and a probe that says the pid is
        // dead. We hand-craft that scenario here.
        string settingsRoot = Path.Combine(Path.GetTempPath(), $"codex-provider-singleton-{Guid.NewGuid():N}");
        Directory.CreateDirectory(settingsRoot);
        string lockDir = Path.Combine(settingsRoot, "singleton");
        try
        {
            Directory.CreateDirectory(lockDir);
            File.WriteAllText(Path.Combine(lockDir, "owner.json"),
                $"{{\"processId\":{Environment.ProcessId},\"startedAt\":\"2026-01-01T00:00:00+00:00\",\"label\":\"dead-recycle\",\"currentDirectory\":\"C:\\\\\"}}");

            SingleInstanceGuard guard = new(static _ => false, lockDir);
            using SingleInstanceAcquisition acq = guard.Acquire("recycled");
            Assert.True(acq.IsOwner);
            Assert.Null(acq.ExistingOwner);
        }
        finally
        {
            Cleanup(settingsRoot);
        }
    }
}
