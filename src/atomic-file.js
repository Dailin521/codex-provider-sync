import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

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
    }
    await faultInjector?.({ point: "before_atomic_replace", filePath: fullPath, tempPath });
    await fs.rename(tempPath, fullPath);
  } catch (error) {
    await fs.rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
}
