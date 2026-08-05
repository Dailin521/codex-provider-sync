import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

const UNSUPPORTED_DIRECTORY_SYNC_CODES = new Set([
  "EACCES",
  "EBADF",
  "EISDIR",
  "EINVAL",
  "ENOTSUP",
  "EPERM"
]);

export async function syncDirectory(
  directoryPath,
  { fsImpl = fs, platform = process.platform } = {}
) {
  let handle;
  try {
    handle = await fsImpl.open(directoryPath, "r");
    await handle.sync();
  } catch (error) {
    // Windows does not consistently allow directories to be opened and
    // flushed through the Node fs API. The staged file itself is still
    // flushed before rename; directory fsync remains mandatory wherever the
    // host supports it.
    if (platform !== "win32" || !UNSUPPORTED_DIRECTORY_SYNC_CODES.has(error?.code)) {
      throw error;
    }
  } finally {
    await handle?.close();
  }
}

export async function writeFileAtomic(
  filePath,
  content,
  encoding = "utf8",
  { faultInjector } = {}
) {
  const fullPath = path.resolve(filePath);
  const directory = path.dirname(fullPath);
  const tempPath = path.join(
    directory,
    `.${path.basename(fullPath)}.provider-sync.${process.pid}.${randomUUID()}.tmp`
  );
  let originalMode = null;
  try {
    originalMode = (await fs.stat(fullPath)).mode;
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }

  try {
    const handle = await fs.open(tempPath, "wx", originalMode ?? 0o600);
    try {
      await faultInjector?.({ point: "before_stage_write", filePath: fullPath, tempPath });
      await handle.writeFile(content, encoding);
      await handle.sync();
    } finally {
      await handle.close();
    }
    if (originalMode !== null) {
      await fs.chmod(tempPath, originalMode);
      const modeHandle = await fs.open(tempPath, "r+");
      try {
        await modeHandle.sync();
      } finally {
        await modeHandle.close();
      }
    }
    await faultInjector?.({ point: "before_atomic_replace", filePath: fullPath, tempPath });
    await fs.rename(tempPath, fullPath);
    await syncDirectory(directory);
  } catch (error) {
    await fs.rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
}
