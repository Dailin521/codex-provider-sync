import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const limits = Object.freeze({ asar: 3 * 1024 * 1024, unpacked: 280 * 1024 * 1024, nsis: 105 * 1024 * 1024, zip: 130 * 1024 * 1024 });
const desktopPackage = JSON.parse(await fs.readFile(path.join(desktopRoot, "package.json"), "utf8"));
const options = new Map();
for (let index = 2; index < process.argv.length; index += 1) {
  const option = process.argv[index];
  assert.ok(["--output", "--version", "--directory"].includes(option) && !options.has(option), `Unknown or repeated argument: ${option}`);
  if (option === "--directory") options.set(option, true);
  else {
    const value = process.argv[++index];
    assert.ok(value?.trim() && !value.startsWith("--"), `${option} needs a value.`);
    options.set(option, value);
  }
}
const outputRoot = options.has("--output") ? path.resolve(options.get("--output")) : path.resolve(desktopRoot, "../../dist-desktop");
const version = options.get("--version") ?? desktopPackage.version;
assert.match(version, /^\d+\.\d+\.\d+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?$/, "Invalid artifact version.");

async function bytesUnder(root) {
  let total = 0;
  for (const entry of await fs.readdir(root, { withFileTypes: true })) {
    const target = path.join(root, entry.name);
    assert.ok(!entry.isSymbolicLink(), "Size measurement refuses linked package entries.");
    total += entry.isDirectory() ? await bytesUnder(target) : entry.isFile() ? (await fs.stat(target)).size : 0;
  }
  return total;
}

const winRoot = path.join(outputRoot, "win-unpacked");
const asarPath = path.join(winRoot, "resources", "app.asar");
const [unpackedBytes, asarBytes, localeNames] = await Promise.all([
  bytesUnder(winRoot),
  fs.stat(asarPath).then((stat) => stat.size),
  fs.readdir(path.join(winRoot, "locales"))
]);
assert.ok(asarBytes <= limits.asar, `app.asar exceeds 3 MiB: ${asarBytes}`);
assert.ok(unpackedBytes <= limits.unpacked, `Windows unpacked directory exceeds 280 MiB: ${unpackedBytes}`);
assert.deepEqual(localeNames.sort(), ["en-US.pak", "zh-CN.pak"], "Chromium locales must contain only en-US and zh-CN.");

let nsisBytes;
let zipBytes;
if (!options.has("--directory")) {
  const setupPath = path.join(outputRoot, `CodexProviderSync-${version}-windows-x64-setup.exe`);
  const archivePath = path.join(outputRoot, `CodexProviderSync-${version}-windows-x64-portable.zip`);
  nsisBytes = await fs.stat(setupPath).then((stat) => stat.size).catch(() => null);
  zipBytes = await fs.stat(archivePath).then((stat) => stat.size).catch(() => null);
  assert.notEqual(nsisBytes, null, "Windows NSIS artifact is missing.");
  assert.notEqual(zipBytes, null, "Windows portable ZIP artifact is missing.");
  assert.ok(nsisBytes <= limits.nsis, `Windows NSIS exceeds 105 MiB: ${nsisBytes}`);
  assert.ok(zipBytes <= limits.zip, `Windows portable ZIP exceeds 130 MiB: ${zipBytes}`);
}

process.stdout.write(`${JSON.stringify({ schemaVersion: 1, outputRoot, version, asarBytes, unpackedBytes, ...(nsisBytes === undefined ? {} : { nsisBytes, zipBytes }), locales: localeNames })}\n`);
