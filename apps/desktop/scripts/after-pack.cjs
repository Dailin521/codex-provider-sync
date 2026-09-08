const fs = require("node:fs/promises");
const path = require("node:path");
const { archiveChromiumNotices } = require("./windows-license-archive.cjs");

const OPTIONAL_VULKAN_RUNTIME = Object.freeze([
  "dxcompiler.dll",
  "dxil.dll",
  "vk_swiftshader.dll",
  "vk_swiftshader_icd.json",
  "vulkan-1.dll"
]);

module.exports = async function afterPack(context) {
  if (context.electronPlatformName !== "win32") return;
  const appOutDir = path.resolve(context.appOutDir);
  const outputRoot = path.resolve(context.packager.config.directories?.output ?? "");
  if (!outputRoot || (appOutDir !== outputRoot && !appOutDir.startsWith(`${outputRoot}${path.sep}`))) {
    throw new Error("Refusing to trim files outside the configured Electron output directory.");
  }
  const [realOutput, realApp] = await Promise.all([fs.realpath(outputRoot), fs.realpath(appOutDir)]);
  if (!realApp.startsWith(`${realOutput}${path.sep}`)) {
    throw new Error("Refusing to trim an output that resolves outside its parent directory.");
  }
  // Do not discard third-party terms to reach a size budget: archive the exact
  // original bytes with an offline reading guide and verify before removal.
  const notices = await archiveChromiumNotices(appOutDir);
  console.info(`Chromium notices archived losslessly: ${notices.originalBytes} -> ${notices.archiveBytes} bytes.`);
  for (const name of OPTIONAL_VULKAN_RUNTIME) {
    await fs.rm(path.join(appOutDir, name), { force: true });
  }
};
