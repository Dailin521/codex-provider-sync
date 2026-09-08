// Build-time only. Keep Chromium's complete notices, byte-for-byte, inside the
// application directory. No separate download or runtime dependency is needed.
const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const { createHash, randomUUID } = require("node:crypto");
const fs = require("node:fs/promises");
const { createRequire } = require("node:module");
const path = require("node:path");
const { promisify } = require("node:util");

const SOURCE = "LICENSES.chromium.html";
const ARCHIVE = "LICENSES.chromium.zip";
const NOTICE = "THIRD-PARTY-NOTICES.txt";
const MANIFEST = "chromium-notices.json";
const LIMIT = 64 * 1024 * 1024;
const exec = promisify(execFile);
const digest = (data) => createHash("sha256").update(data).digest("hex");

async function run7zip(args, cwd) {
  // Pinned electron-builder 26.x owns this build tool; never ship it in ASAR.
  const builderRequire = createRequire(require.resolve("electron-builder"));
  const binary = await builderRequire("app-builder-lib/out/toolsets/7zip.js").getPath7za();
  return (await exec(binary, args, { cwd, windowsHide: true, timeout: 60_000, maxBuffer: LIMIT, encoding: "buffer" })).stdout;
}

async function regularFile(filePath, limit = LIMIT) {
  const stat = await fs.lstat(filePath);
  assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size > 0 && stat.size <= limit, `Invalid notices file: ${path.basename(filePath)}`);
  return fs.readFile(filePath);
}

async function verifyLicenseArchive(appRoot, expectedSource) {
  const manifest = JSON.parse(await regularFile(path.join(appRoot, MANIFEST), 2048));
  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.source, SOURCE);
  assert.equal(manifest.archive, ARCHIVE);
  assert.match(manifest.sha256, /^[a-f0-9]{64}$/);
  assert.ok(Number.isSafeInteger(manifest.originalBytes) && manifest.originalBytes > 0 && manifest.originalBytes <= LIMIT);
  await regularFile(path.join(appRoot, ARCHIVE));
  await run7zip(["t", ARCHIVE], appRoot);
  const restored = await run7zip(["e", "-so", ARCHIVE, SOURCE], appRoot);
  assert.equal(restored.length, manifest.originalBytes);
  assert.equal(digest(restored), manifest.sha256, "Archived Chromium notices changed.");
  if (expectedSource) assert.ok(restored.equals(expectedSource), "Chromium notices must remain byte-identical.");
  const notice = (await regularFile(path.join(appRoot, NOTICE), 4096)).toString("utf8");
  assert.ok(notice.includes(ARCHIVE) && notice.includes(manifest.sha256));
  return Object.freeze({ originalBytes: restored.length, archiveBytes: (await fs.stat(path.join(appRoot, ARCHIVE))).size, sha256: manifest.sha256 });
}

async function archiveChromiumNotices(appRoot) {
  const source = await regularFile(path.join(appRoot, SOURCE));
  const temporary = `.chromium-notices-${randomUUID()}.zip`;
  try {
    await run7zip(["a", "-tzip", "-mx=9", "-mm=Deflate", temporary, SOURCE], appRoot);
    const restored = await run7zip(["e", "-so", temporary, SOURCE], appRoot);
    assert.ok(restored.equals(source), "Refusing to replace notices without a verified lossless archive.");
    const sha256 = digest(source);
    await fs.rename(path.join(appRoot, temporary), path.join(appRoot, ARCHIVE));
    await fs.writeFile(path.join(appRoot, MANIFEST), `${JSON.stringify({ schemaVersion: 1, source: SOURCE, archive: ARCHIVE, originalBytes: source.length, sha256 })}\n`);
    await fs.writeFile(path.join(appRoot, NOTICE), [
      "Third-party notices / 第三方许可证说明", "",
      `The complete, unmodified Chromium notices are included in ${ARCHIVE}.`,
      `Open the ZIP in Windows Explorer, extract ${SOURCE}, then open it in your browser.`,
      "No Internet connection is required. Keep this archive with the application.", "",
      `完整 Chromium 许可证原文保存在 ${ARCHIVE}，内容未删减。`,
      `用资源管理器打开 ZIP，解压 ${SOURCE} 后用浏览器查看。`,
      "无需联网。请将该归档与程序一起保留。", "",
      `Original SHA256: ${sha256}`, ""
    ].join("\r\n"), "utf8");
    const result = await verifyLicenseArchive(appRoot, source);
    await fs.rm(path.join(appRoot, SOURCE));
    return result;
  } finally {
    await fs.rm(path.join(appRoot, temporary), { force: true });
  }
}

module.exports = { archiveChromiumNotices, verifyLicenseArchive };
