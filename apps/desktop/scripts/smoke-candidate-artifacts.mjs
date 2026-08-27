import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  auditExtractedApp,
  RELEASE_REPOSITORY_ROOT,
  RELEASE_TARGETS,
  sha256File
} from "./release-audit.mjs";

const VERSION_PATTERN = /^1\.0\.0-(?:alpha|beta|rc)\.\d+$/;
const LINUX_SANDBOX_HELPER = fileURLToPath(new URL("./configure-linux-sandbox.mjs", import.meta.url));
const target = process.env.CPS_CANDIDATE_TARGET;
const version = process.env.CPS_DESKTOP_VERSION;
const descriptor = RELEASE_TARGETS[target];
if (!descriptor) throw new Error("CPS_CANDIDATE_TARGET is invalid.");
if (!VERSION_PATTERN.test(version || "")) throw new Error("CPS_DESKTOP_VERSION is invalid.");
if (process.platform !== descriptor.platform || process.arch !== descriptor.arch) {
  throw new Error(`Candidate ${target} smoke must run on native ${descriptor.platform}/${descriptor.arch}.`);
}

const candidateRoot = path.join(RELEASE_REPOSITORY_ROOT, "artifacts", "c9", target);
const assetsRoot = path.join(candidateRoot, "assets");
const metadataRoot = path.join(candidateRoot, "metadata");
const stagingPath = path.join(metadataRoot, "candidate-staging.v1.json");
const auditPath = path.join(metadataRoot, "audit-report.v1.json");
const staging = JSON.parse(await fs.readFile(stagingPath, "utf8"));
const expectedAudit = JSON.parse(await fs.readFile(auditPath, "utf8"));
assert.equal(staging.schemaVersion, 1);
assert.equal(staging.scope, "ci-candidate-staging");
assert.equal(staging.releaseAuthorized, false);
assert.equal(staging.target, target);
assert.equal(staging.version, version);
assert.equal(expectedAudit.target, target);
assert.equal(expectedAudit.version, version);
assert.equal(expectedAudit.buildId, staging.buildId);
assert.deepEqual(
  staging.assets.map((asset) => asset.name).sort(),
  descriptor.assets(version).sort(),
  "Staged candidate assets do not match the release target."
);
const containerRecords = [];
const tempBase = path.resolve(os.tmpdir());
const tempRoot = await fs.mkdtemp(path.join(tempBase, "cps-c9-artifact-smoke-"));

function sorted(value) {
  if (Array.isArray(value)) return value.map(sorted);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sorted(value[key])]));
}

async function writeJson(filePath, value) {
  await fs.writeFile(filePath, `${JSON.stringify(sorted(value), null, 2)}\n`, { encoding: "utf8", flag: "wx" });
}

async function fileRecord(filePath, name) {
  const stat = await fs.stat(filePath);
  assert.equal(stat.isFile(), true, `Candidate metadata is not a file: ${name}`);
  return Object.freeze({ name, sizeBytes: stat.size, sha256: await sha256File(filePath) });
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || RELEASE_REPOSITORY_ROOT,
    env: options.env || process.env,
    encoding: "utf8",
    stdio: options.inherit ? "inherit" : "pipe",
    maxBuffer: 16 * 1024 * 1024
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${path.basename(command)} failed with exit code ${result.status}: ${String(result.stderr).trim()}`);
  }
  return result;
}

async function existingFile(candidates) {
  for (const candidate of candidates) {
    try {
      if ((await fs.stat(candidate)).isFile()) return candidate;
    } catch (error) {
      if (!error || typeof error !== "object" || error.code !== "ENOENT") throw error;
    }
  }
  return null;
}

async function findNamedFile(root, names) {
  const found = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile() && names.includes(entry.name)) found.push(absolute);
    }
  }
  await visit(root);
  if (found.length !== 1) throw new Error(`Expected one packaged executable under ${path.basename(root)}, found ${found.length}.`);
  return found[0];
}

async function waitForRemoval(targetPath, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await fs.lstat(targetPath);
    } catch (error) {
      if (error && typeof error === "object" && error.code === "ENOENT") return;
      throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for uninstall cleanup: ${path.basename(targetPath)}`);
}

function runProductSmoke(executable) {
  const npmCli = process.env.npm_execpath;
  if (!npmCli) throw new Error("npm_execpath is required for candidate smoke.");
  run(process.execPath, [
    npmCli,
    "run",
    "test:e2e:production",
    "--workspace",
    "@codex-provider-sync/desktop"
  ], {
    inherit: true,
    env: {
      ...process.env,
      CPS_DESKTOP_EXECUTABLE: executable,
      CPS_DESKTOP_WINDOW_DISPLAY: "hidden"
    }
  });
}

async function configureLinuxSandbox(appRoot) {
  if (process.platform !== "linux") {
    if (process.env.CPS_LINUX_SANDBOX_SETUP) {
      throw new Error("CPS_LINUX_SANDBOX_SETUP is supported only on Linux.");
    }
    return;
  }
  const realTempRoot = await fs.realpath(tempRoot);
  const realAppRoot = await fs.realpath(path.resolve(appRoot));
  const relativeAppRoot = path.relative(realTempRoot, realAppRoot);
  if (!relativeAppRoot
      || relativeAppRoot.startsWith(`..${path.sep}`)
      || relativeAppRoot === ".."
      || path.isAbsolute(relativeAppRoot)) {
    throw new Error("Refusing to configure a Linux sandbox outside the candidate smoke directory.");
  }
  const sandboxPath = path.join(realAppRoot, "chrome-sandbox");
  const before = await fs.lstat(sandboxPath);
  if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1) {
    throw new Error("The packaged Linux chrome-sandbox must be one regular, unlinked file.");
  }
  if (before.uid === 0 && (before.mode & 0o7777) === 0o4755) return;
  if (process.env.CPS_LINUX_SANDBOX_SETUP !== "setuid") {
    throw new Error(
      "The Linux candidate sandbox is not configured; set CPS_LINUX_SANDBOX_SETUP=setuid in an authorized CI environment."
    );
  }
  run("sudo", ["--", process.execPath, LINUX_SANDBOX_HELPER, realAppRoot, "chrome-sandbox"]);
  const after = await fs.lstat(sandboxPath);
  if (after.uid !== 0 || (after.mode & 0o7777) !== 0o4755 || after.nlink !== 1) {
    throw new Error("The packaged Linux chrome-sandbox failed its owner/mode verification.");
  }
}

async function inspectAndSmokeContainer({ appRoot, executable, assetName, containerKind }) {
  const audit = await auditExtractedApp({
    appRoot,
    target,
    version,
    buildId: staging.buildId
  });
  assert.deepEqual(audit, expectedAudit, `Final ${containerKind} container differs from the audited app.`);
  await configureLinuxSandbox(appRoot);
  runProductSmoke(executable);
  return Object.freeze({
    assetName,
    containerKind,
    asarSha256: audit.asar.sha256,
    asarHeaderSha256: audit.asar.headerSha256,
    asarEntryCount: audit.asar.entryCount,
    asarIntegrityEntryCount: audit.asar.integrityEntryCount,
    asarRuntimeIntegrity: audit.asar.runtimeIntegrity,
    nativeBindingSha256: audit.nativeDriver.bindingSha256,
    nativeDriverLoaded: true,
    fixtureStatusVerified: true,
    syncRestoreVerified: true,
    gracefulExitVerified: true
  });
}

async function smokeWindows() {
  const [setupName, zipName] = descriptor.assets(version);
  const zipRoot = path.join(tempRoot, "portable");
  await fs.mkdir(zipRoot);
  run("tar.exe", ["-xf", path.join(assetsRoot, zipName), "-C", zipRoot]);
  const portableExecutable = await findNamedFile(zipRoot, ["Codex Provider Sync.exe", "codex-provider-sync.exe"]);
  containerRecords.push(await inspectAndSmokeContainer({
    appRoot: path.dirname(portableExecutable),
    executable: portableExecutable,
    assetName: zipName,
    containerKind: "zip"
  }));

  const installRoot = path.join(tempRoot, "installed");
  await fs.mkdir(installRoot);
  run(path.join(assetsRoot, setupName), ["/S", `/D=${installRoot}`]);
  const installedExecutable = await findNamedFile(installRoot, ["Codex Provider Sync.exe", "codex-provider-sync.exe"]);
  const setupRecord = await inspectAndSmokeContainer({
    appRoot: path.dirname(installedExecutable),
    executable: installedExecutable,
    assetName: setupName,
    containerKind: "nsis"
  });
  const uninstaller = await existingFile([
    path.join(installRoot, "Uninstall Codex Provider Sync.exe"),
    path.join(installRoot, "Uninstall codex-provider-sync.exe")
  ]);
  if (!uninstaller) throw new Error("NSIS candidate did not install its uninstaller.");
  run(uninstaller, ["/S"]);
  await waitForRemoval(installRoot);
  containerRecords.push(Object.freeze({ ...setupRecord, uninstallVerified: true }));
}

async function smokeMac() {
  const [dmgName, zipName] = descriptor.assets(version);
  const zipRoot = path.join(tempRoot, "zip");
  await fs.mkdir(zipRoot);
  run("ditto", ["-x", "-k", path.join(assetsRoot, zipName), zipRoot]);
  const zipApp = path.join(zipRoot, "Codex Provider Sync.app");
  containerRecords.push(await inspectAndSmokeContainer({
    appRoot: zipApp,
    executable: path.join(zipApp, "Contents", "MacOS", "Codex Provider Sync"),
    assetName: zipName,
    containerKind: "zip"
  }));

  const mountRoot = path.join(tempRoot, "dmg-mount");
  const copyRoot = path.join(tempRoot, "dmg-copy");
  await fs.mkdir(mountRoot);
  await fs.mkdir(copyRoot);
  run("hdiutil", ["attach", "-nobrowse", "-readonly", "-mountpoint", mountRoot, path.join(assetsRoot, dmgName)]);
  try {
    await fs.cp(
      path.join(mountRoot, "Codex Provider Sync.app"),
      path.join(copyRoot, "Codex Provider Sync.app"),
      { recursive: true, errorOnExist: true }
    );
  } finally {
    run("hdiutil", ["detach", mountRoot]);
  }
  const dmgApp = path.join(copyRoot, "Codex Provider Sync.app");
  containerRecords.push(await inspectAndSmokeContainer({
    appRoot: dmgApp,
    executable: path.join(dmgApp, "Contents", "MacOS", "Codex Provider Sync"),
    assetName: dmgName,
    containerKind: "dmg"
  }));
}

async function smokeLinux() {
  const [appImageName, debName] = descriptor.assets(version);
  const appImage = path.join(assetsRoot, appImageName);
  await fs.chmod(appImage, 0o755);
  const appImageRoot = path.join(tempRoot, "appimage");
  await fs.mkdir(appImageRoot);
  run(appImage, ["--appimage-extract"], { cwd: appImageRoot });
  const appImageExecutable = await findNamedFile(path.join(appImageRoot, "squashfs-root"), ["codex-provider-sync"]);
  containerRecords.push(await inspectAndSmokeContainer({
    appRoot: path.dirname(appImageExecutable),
    executable: appImageExecutable,
    assetName: appImageName,
    containerKind: "appimage"
  }));

  const debRoot = path.join(tempRoot, "deb");
  await fs.mkdir(debRoot);
  run("dpkg-deb", ["-x", path.join(assetsRoot, debName), debRoot]);
  const debExecutable = await findNamedFile(debRoot, ["codex-provider-sync"]);
  containerRecords.push(await inspectAndSmokeContainer({
    appRoot: path.dirname(debExecutable),
    executable: debExecutable,
    assetName: debName,
    containerKind: "deb"
  }));
}

async function finalizeCandidate() {
  containerRecords.sort((left, right) => left.assetName.localeCompare(right.assetName));
  assert.deepEqual(
    containerRecords.map((record) => record.assetName),
    descriptor.assets(version).sort(),
    "Every release container must be audited and smoked exactly once."
  );
  for (const asset of staging.assets) {
    const assetPath = path.join(assetsRoot, asset.name);
    const stat = await fs.stat(assetPath);
    assert.equal(stat.size, asset.sizeBytes, `Candidate asset size changed: ${asset.name}`);
    assert.equal(await sha256File(assetPath), asset.sha256, `Candidate asset hash changed: ${asset.name}`);
  }

  const verifiedAt = new Date().toISOString();
  const containerPath = path.join(metadataRoot, "container-verification.v1.json");
  const report = {
    schemaVersion: 1,
    scope: "final-release-containers",
    target,
    version,
    buildId: staging.buildId,
    commit: staging.commit,
    verifiedAt,
    containers: containerRecords
  };
  await writeJson(containerPath, report);

  const metadata = await Promise.all([
    fileRecord(auditPath, "metadata/audit-report.v1.json"),
    fileRecord(path.join(metadataRoot, "sbom.cyclonedx.json"), "metadata/sbom.cyclonedx.json"),
    fileRecord(stagingPath, "metadata/candidate-staging.v1.json"),
    fileRecord(containerPath, "metadata/container-verification.v1.json")
  ]);
  metadata.sort((left, right) => left.name.localeCompare(right.name));
  const containerMetadata = metadata.find((entry) => entry.name.endsWith("container-verification.v1.json"));
  assert.ok(containerMetadata);
  const { scope: _stagingScope, ...stagingFields } = staging;
  const manifest = {
    ...stagingFields,
    scope: "ci-candidate",
    verifiedAt,
    containerVerification: {
      reportSha256: containerMetadata.sha256,
      containerCount: containerRecords.length,
      fixtureStatus: true,
      syncRestore: true,
      nativeDriver: true,
      gracefulExit: true,
      uninstall: descriptor.platform === "win32"
    },
    metadata
  };
  const manifestPath = path.join(metadataRoot, "release-manifest.v1.json");
  await writeJson(manifestPath, manifest);

  const checksumEntries = [
    ...staging.assets.map((asset) => ({ name: `assets/${asset.name}`, sha256: asset.sha256 })),
    ...metadata.map(({ name, sha256 }) => ({ name, sha256 })),
    { name: "metadata/release-manifest.v1.json", sha256: await sha256File(manifestPath) }
  ].sort((left, right) => left.name.localeCompare(right.name));
  await fs.writeFile(
    path.join(metadataRoot, "SHA256SUMS.txt"),
    `${checksumEntries.map((entry) => `${entry.sha256}  ${entry.name}`).join("\n")}\n`,
    { encoding: "utf8", flag: "wx" }
  );
}

try {
  if (descriptor.platform === "win32") await smokeWindows();
  else if (descriptor.platform === "darwin") await smokeMac();
  else await smokeLinux();
  await finalizeCandidate();
  process.stdout.write(`Candidate artifact smoke passed: ${target} ${version}\n`);
} finally {
  const resolved = path.resolve(tempRoot);
  if (path.dirname(resolved) !== tempBase || !path.basename(resolved).startsWith("cps-c9-artifact-smoke-")) {
    throw new Error("Refusing to remove an unexpected candidate smoke directory.");
  }
  await fs.rm(resolved, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 });
}
