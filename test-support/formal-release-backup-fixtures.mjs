import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { getStatus, runRestore } from "../src/public-api.js";
import { defaultBackupRoot } from "../src/constants.js";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifestPath = path.join(repositoryRoot, "test-support", "formal-release-assets.v1.json");
const reportPath = path.join(
  repositoryRoot,
  "artifacts",
  "test-fixtures",
  "historical-formal-release-backup-evidence.json"
);

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function redact(value, redactions = []) {
  let output = String(value ?? "");
  for (const item of redactions) {
    if (typeof item === "string" && item) output = output.replaceAll(item, "<redacted>");
  }
  return output.replace(/FORMAL_RELEASE_AUTH_CANARY_[a-f0-9]+/gi, "<redacted-canary>");
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    env: options.env ?? process.env,
    encoding: "utf8",
    windowsHide: true,
    maxBuffer: 16 * 1024 * 1024,
    timeout: options.timeoutMs ?? 10 * 60_000
  });
  assert.equal(result.error, undefined, result.error?.message);
  const failureOutput = redact([result.stdout, result.stderr].filter(Boolean).join("\n"), options.redactions);
  assert.equal(result.status, 0, failureOutput);
  return result.stdout.trim();
}

function githubApiHeaders() {
  return {
    accept: "application/vnd.github+json",
    "user-agent": "codex-provider-sync-formal-release-fixture",
    "x-github-api-version": "2022-11-28"
  };
}

function allowlistedWindowsEnvironment(overrides = {}) {
  const environment = {};
  for (const name of [
    "ComSpec",
    "NUMBER_OF_PROCESSORS",
    "OS",
    "Path",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_IDENTIFIER",
    "PROCESSOR_LEVEL",
    "PROCESSOR_REVISION",
    "SystemDrive",
    "SystemRoot",
    "WINDIR"
  ]) {
    if (typeof process.env[name] === "string" && process.env[name]) environment[name] = process.env[name];
  }
  return { ...environment, ...overrides };
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: githubApiHeaders(),
    redirect: "error",
    signal: AbortSignal.timeout(60_000)
  });
  assert.equal(response.ok, true, `GitHub API returned HTTP ${response.status}.`);
  return response.json();
}

async function verifyDownloadedAsset(asset, destination) {
  const info = await fs.lstat(destination);
  assert.equal(info.isFile(), true, `${asset.name} is not a regular file.`);
  assert.equal(info.isSymbolicLink(), false, `${asset.name} is a link.`);
  assert.equal(info.size, asset.size, `${asset.name} size changed.`);
  const bytes = await fs.readFile(destination);
  assert.equal(bytes.length, asset.size, `${asset.name} size changed.`);
  assert.equal(sha256(bytes), asset.sha256, `${asset.name} digest changed.`);
  return bytes;
}

function verifyReleaseAsset(actual, expected) {
  assert.equal(actual.id, expected.id, `${expected.name} asset id changed.`);
  assert.equal(actual.name, expected.name, `${expected.name} asset name changed.`);
  assert.equal(actual.size, expected.size, `${expected.name} asset size changed.`);
  assert.equal(actual.digest, `sha256:${expected.sha256}`, `${expected.name} API digest changed.`);
  assert.equal(actual.browser_download_url, expected.url, `${expected.name} download URL changed.`);
  assert.equal(actual.state, "uploaded", `${expected.name} is not uploaded.`);
}

async function verifyFormalRelease(manifest) {
  const repositoryApi = `https://api.github.com/repos/${manifest.repository}`;
  const release = await fetchJson(`${repositoryApi}/releases/tags/${manifest.release.tag}`);
  assert.equal(release.id, manifest.release.id);
  assert.equal(release.tag_name, manifest.release.tag);
  assert.equal(release.name, manifest.release.name);
  assert.equal(release.published_at, manifest.release.publishedAt);
  assert.equal(release.draft, manifest.release.draft);
  assert.equal(release.prerelease, manifest.release.prerelease);
  assert.equal(release.author?.login, manifest.release.uploader);
  for (const asset of Object.values(manifest.assets)) {
    const actual = release.assets.find((candidate) => candidate.id === asset.id);
    assert.ok(actual, `Release asset ${asset.name} is missing.`);
    verifyReleaseAsset(actual, asset);
  }

  const ref = await fetchJson(`${repositoryApi}/git/ref/tags/${manifest.release.tag}`);
  assert.equal(ref.object?.type, "tag");
  assert.equal(ref.object?.sha, manifest.release.tagObjectSha);
  const tag = await fetchJson(`${repositoryApi}/git/tags/${manifest.release.tagObjectSha}`);
  assert.equal(tag.object?.type, "commit");
  assert.equal(tag.object?.sha, manifest.release.commit);
  assert.equal(tag.verification?.verified, manifest.release.tagSigned);
}

async function verifyArchive(extractRoot, entries) {
  const actualNames = (await fs.readdir(extractRoot)).sort((left, right) => left.localeCompare(right));
  const expectedNames = entries.map((entry) => entry.name).sort((left, right) => left.localeCompare(right));
  assert.deepEqual(actualNames, expectedNames, "The formal Automation archive entry set changed.");
  for (const entry of entries) {
    const filePath = path.join(extractRoot, entry.name);
    const info = await fs.lstat(filePath);
    assert.equal(info.isFile(), true, `${entry.name} is not a regular file.`);
    assert.equal(info.isSymbolicLink(), false, `${entry.name} is a link.`);
    assert.equal(info.size, entry.size, `${entry.name} size changed.`);
    assert.equal(sha256(await fs.readFile(filePath)), entry.sha256, `${entry.name} digest changed.`);
  }
}

function verifyArchiveListing(listing, entries) {
  const actualNames = listing.split(/\r?\n/).map((entry) => entry.trim()).filter(Boolean);
  const expectedNames = entries.map((entry) => entry.name);
  assert.equal(new Set(actualNames).size, actualNames.length, "The formal Automation archive has duplicate entries.");
  for (const entry of actualNames) {
    const normalized = entry.replaceAll("\\", "/");
    assert.equal(path.posix.isAbsolute(normalized), false, `The formal Automation archive has an absolute entry: ${entry}`);
    assert.equal(normalized.split("/").includes(".."), false, `The formal Automation archive escapes its root: ${entry}`);
    assert.equal(normalized.includes(":"), false, `The formal Automation archive has a drive or ADS entry: ${entry}`);
  }
  assert.deepEqual(
    [...actualNames].sort((left, right) => left.localeCompare(right)),
    [...expectedNames].sort((left, right) => left.localeCompare(right)),
    "The formal Automation archive entry set changed before extraction."
  );
}

function isolatedAutomationEnvironment(root, fixture) {
  return allowlistedWindowsEnvironment({
    HOME: path.join(root, "process-home"),
    USERPROFILE: path.join(root, "process-home"),
    APPDATA: path.join(root, "process-appdata"),
    LOCALAPPDATA: path.join(root, "process-localappdata"),
    TEMP: path.join(root, "process-temp"),
    TMP: path.join(root, "process-temp"),
    CODEX_HOME: fixture.codexHome,
    CODEX_SQLITE_HOME: fixture.sqliteHome
  });
}

function runAutomation(executable, args, environment, redactions) {
  const stdout = run(executable, args, { env: environment, redactions });
  assert.equal(stdout.startsWith("{"), true, "Automation stdout is not one JSON document.");
  assert.equal(stdout.endsWith("}"), true, "Automation stdout is not one JSON document.");
  let response;
  try {
    response = JSON.parse(stdout);
  } catch (error) {
    assert.fail(`Automation stdout is not valid JSON: ${error?.message ?? "parse failed"}; output=${redact(stdout, redactions)}`);
  }
  const safeStdout = redact(stdout, redactions);
  assert.equal(response.protocolVersion, "0.4");
  assert.equal(response.result, "success", safeStdout);
  assert.equal(response.exitCode, 0, safeStdout);
  return response;
}

async function treeSha256(root, forbiddenMarker) {
  const records = [];
  async function visit(directory) {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const absolute = path.join(directory, entry.name);
      const relative = path.relative(root, absolute).replaceAll("\\", "/");
      assert.equal(entry.isSymbolicLink(), false, `formal Release backup contains a link: ${relative}`);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) {
        const bytes = await fs.readFile(absolute);
        assert.equal(
          bytes.includes(Buffer.from(forbiddenMarker, "utf8")),
          false,
          `formal Release backup copied the synthetic credential marker: ${relative}`
        );
        records.push([relative, sha256(bytes)]);
      } else {
        assert.fail(`formal Release backup contains an unsupported entry: ${relative}`);
      }
    }
  }
  await visit(root);
  return sha256(JSON.stringify(records));
}

function sqliteProvider(databasePath, nextProvider) {
  const database = new DatabaseSync(databasePath);
  try {
    if (nextProvider) {
      database.prepare("UPDATE threads SET model_provider = ? WHERE id = ?")
        .run(nextProvider, "formal-release-synthetic-thread");
    }
    return database.prepare("SELECT model_provider AS provider FROM threads WHERE id = ?")
      .get("formal-release-synthetic-thread").provider;
  } finally {
    database.close();
  }
}

async function createSyntheticHome(root) {
  const codexHome = path.join(root, "codex-home");
  const sqliteHome = path.join(codexHome, "sqlite");
  const rolloutPath = path.join(
    codexHome,
    "sessions",
    "2026",
    "08",
    "28",
    "rollout-formal-release-synthetic.jsonl"
  );
  const databasePath = path.join(sqliteHome, "state_5.sqlite");
  const configPath = path.join(codexHome, "config.toml");
  const authPath = path.join(codexHome, "auth.json");
  const config = 'model_provider = "openai"\n';
  const authCanary = `FORMAL_RELEASE_AUTH_CANARY_${crypto.randomBytes(16).toString("hex")}`;
  const rollout = `${JSON.stringify({
    type: "session_meta",
    timestamp: "2026-08-28T00:00:00.000Z",
    payload: {
      id: "formal-release-synthetic-thread",
      cwd: "C:\\synthetic\\formal-release",
      model_provider: "apigather"
    }
  })}\n`;
  await fs.mkdir(path.dirname(rolloutPath), { recursive: true });
  await fs.mkdir(sqliteHome, { recursive: true });
  await fs.mkdir(path.join(codexHome, "archived_sessions"), { recursive: true });
  await fs.writeFile(configPath, config, "utf8");
  await fs.writeFile(rolloutPath, rollout, "utf8");
  await fs.writeFile(authPath, `${JSON.stringify({ token: authCanary })}\n`, "utf8");
  const database = new DatabaseSync(databasePath);
  try {
    database.exec(`
      CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        model_provider TEXT,
        cwd TEXT NOT NULL DEFAULT '',
        archived INTEGER NOT NULL DEFAULT 0,
        first_user_message TEXT NOT NULL DEFAULT '',
        model TEXT,
        has_user_event INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL DEFAULT 0
      );
    `);
    database.prepare(`
      INSERT INTO threads (
        id, model_provider, cwd, archived, first_user_message, model,
        has_user_event, updated_at, updated_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "formal-release-synthetic-thread",
      "apigather",
      "C:\\synthetic\\formal-release",
      0,
      "",
      null,
      0,
      1787875200,
      1787875200000
    );
  } finally {
    database.close();
  }
  return { codexHome, sqliteHome, rolloutPath, databasePath, configPath, authPath, config, rollout, authCanary };
}

test("current Node restores a checksum-bound backup produced by the hosted v0.4.1 formal Release", async (t) => {
  assert.equal(process.platform, "win32", "Formal Release binary evidence is a Windows CI fixture.");
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.repository, "Dailin521/codex-provider-sync");
  await verifyFormalRelease(manifest);

  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cps-formal-release-backup-"));
  t.after(() => fs.rm(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }));
  const downloadRoot = path.join(root, "download");
  const extractRoot = path.join(root, "extract");
  await fs.mkdir(downloadRoot, { recursive: true });
  await fs.mkdir(extractRoot, { recursive: true });
  const zipPath = path.join(downloadRoot, manifest.assets.automationZip.name);
  const checksumPath = path.join(downloadRoot, manifest.assets.automationChecksum.name);
  const releaseChecksumsPath = path.join(downloadRoot, manifest.assets.releaseChecksums.name);
  const toolEnvironment = allowlistedWindowsEnvironment({
    TEMP: path.join(root, "process-temp"),
    TMP: path.join(root, "process-temp")
  });
  await fs.mkdir(toolEnvironment.TEMP, { recursive: true });
  const downloadEnvironment = process.env.GITHUB_ACTIONS === "true"
    ? { ...toolEnvironment }
    : { ...process.env };
  if (process.env.GITHUB_ACTIONS === "true") {
    downloadEnvironment.GH_CONFIG_DIR = path.join(root, "gh-config");
    await fs.mkdir(downloadEnvironment.GH_CONFIG_DIR, { recursive: true });
  }
  if (typeof process.env.GITHUB_TOKEN === "string" && process.env.GITHUB_TOKEN) {
    downloadEnvironment.GH_TOKEN = process.env.GITHUB_TOKEN;
  }
  run("gh", [
    "release", "download", manifest.release.tag,
    "--repo", manifest.repository,
    "--pattern", manifest.assets.automationZip.name,
    "--pattern", manifest.assets.automationChecksum.name,
    "--pattern", manifest.assets.releaseChecksums.name,
    "--dir", downloadRoot
  ], { env: downloadEnvironment, redactions: [root] });
  await Promise.all([
    verifyDownloadedAsset(manifest.assets.automationZip, zipPath),
    verifyDownloadedAsset(manifest.assets.automationChecksum, checksumPath),
    verifyDownloadedAsset(manifest.assets.releaseChecksums, releaseChecksumsPath)
  ]);
  const expectedChecksumLine = `${manifest.assets.automationZip.sha256}  ${manifest.assets.automationZip.name}`;
  assert.equal((await fs.readFile(checksumPath, "utf8")).trim(), expectedChecksumLine);
  assert.equal(
    (await fs.readFile(releaseChecksumsPath, "utf8")).split(/\r?\n/).includes(expectedChecksumLine),
    true,
    "checksums.txt does not bind the Automation ZIP."
  );
  const archiveListing = run("tar", ["-tf", zipPath], { env: toolEnvironment, redactions: [root] });
  verifyArchiveListing(archiveListing, manifest.archiveEntries);
  run("tar", ["-xf", zipPath, "-C", extractRoot], { env: toolEnvironment, redactions: [root] });
  await verifyArchive(extractRoot, manifest.archiveEntries);

  const executable = path.join(extractRoot, "CodexProviderSync.Automation.exe");
  const escapedExecutable = executable.replaceAll("'", "''");
  const signatureStatus = run("pwsh", [
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    `(Get-AuthenticodeSignature -LiteralPath '${escapedExecutable}').Status.ToString()`
  ], { env: toolEnvironment, redactions: [root] });
  assert.equal(signatureStatus, "NotSigned", "The frozen formal Release signature expectation changed.");

  const fixture = await createSyntheticHome(path.join(root, "fixture"));
  const ledgerRoot = path.join(root, "plan-ledger");
  const planPath = path.join(root, "sync-plan.json");
  for (const directory of [
    path.join(root, "process-home"),
    path.join(root, "process-appdata"),
    path.join(root, "process-localappdata"),
    path.join(root, "process-temp")
  ]) await fs.mkdir(directory, { recursive: true });
  const environment = isolatedAutomationEnvironment(root, fixture);
  const executableEntry = manifest.archiveEntries.find((entry) => entry.name === path.basename(executable));
  assert.ok(executableEntry, "The pinned archive manifest has no Automation executable.");
  const redactions = [root, fixture.codexHome, fixture.sqliteHome, fixture.authCanary];
  await verifyDownloadedAsset(executableEntry, executable);
  const describe = runAutomation(executable, ["describe"], environment, redactions);
  assert.equal(describe.data?.explicitApplyRequired, true);
  assert.equal(describe.data?.exactPlanDigestRequired, true);

  const commonArgs = [
    "--codex-home", fixture.codexHome,
    "--sqlite-home", fixture.sqliteHome,
    "--ledger-root", ledgerRoot,
    "--provider", "openai"
  ];
  const planned = runAutomation(executable, ["plan", "--operation", "sync", ...commonArgs], environment, redactions);
  assert.match(planned.data?.digest ?? "", /^[a-f0-9]{64}$/);
  await fs.writeFile(planPath, JSON.stringify(planned.data), { encoding: "utf8", flag: "wx" });
  await verifyDownloadedAsset(executableEntry, executable);
  const applied = runAutomation(executable, [
    "sync",
    ...commonArgs,
    "--apply",
    "--plan", planPath,
    "--plan-digest", planned.data.digest
  ], environment, redactions);
  assert.equal(applied.data?.applied, true);
  const producedBackupDir = applied.data?.result?.backupDir;
  assert.equal(typeof producedBackupDir, "string");
  assert.equal(path.isAbsolute(producedBackupDir), true);
  const managedRoot = await fs.realpath(defaultBackupRoot(fixture.codexHome));
  const backupDir = await fs.realpath(producedBackupDir);
  assert.equal(path.dirname(backupDir).toLowerCase(), managedRoot.toLowerCase());
  const metadataBytes = await fs.readFile(path.join(backupDir, "metadata.json"));
  const metadata = JSON.parse(metadataBytes);
  assert.equal(metadata.version, 2);
  assert.equal(metadata.namespace, "provider-sync");
  assert.deepEqual(metadata.sqliteDbFiles, ["state_5.sqlite"]);
  assert.equal(metadata.changedSessionFiles, 1);
  for (const relativePath of [
    "config.toml",
    "session-meta-backup.json",
    path.join("db", "sqlite-home", "state_5.sqlite")
  ]) {
    const info = await fs.lstat(path.join(backupDir, relativePath));
    assert.equal(info.isFile(), true, `Formal Release backup is missing ${relativePath}.`);
    assert.equal(info.isSymbolicLink(), false, `Formal Release backup linked ${relativePath}.`);
  }
  assert.equal(sqliteProvider(fixture.databasePath), "openai");
  assert.equal((await fs.readFile(fixture.rolloutPath, "utf8")).includes('"model_provider":"openai"'), true);
  assert.equal(await fs.readFile(fixture.authPath, "utf8"), `${JSON.stringify({ token: fixture.authCanary })}\n`);
  const producedBackupTreeSha256 = await treeSha256(backupDir, fixture.authCanary);

  await fs.writeFile(fixture.configPath, 'model_provider = "relay"\n', "utf8");
  await fs.writeFile(fixture.rolloutPath, fixture.rollout.replace("apigather", "relay"), "utf8");
  assert.equal(sqliteProvider(fixture.databasePath, "relay"), "relay");
  await runRestore({
    codexHome: fixture.codexHome,
    sqliteHome: fixture.sqliteHome,
    backupDir,
    restoreConfig: true,
    restoreDatabase: true,
    restoreSessions: true
  });
  assert.equal(await fs.readFile(fixture.configPath, "utf8"), fixture.config);
  assert.equal(await fs.readFile(fixture.rolloutPath, "utf8"), fixture.rollout);
  assert.equal(sqliteProvider(fixture.databasePath), "apigather");
  assert.equal(await fs.readFile(fixture.authPath, "utf8"), `${JSON.stringify({ token: fixture.authCanary })}\n`);
  const status = await getStatus({ codexHome: fixture.codexHome, sqliteHome: fixture.sqliteHome });
  assert.equal(status.pendingTransactions.length, 0);

  const workflow = process.env.GITHUB_ACTIONS === "true"
    ? {
        repository: process.env.GITHUB_REPOSITORY,
        runId: process.env.GITHUB_RUN_ID,
        runAttempt: Number(process.env.GITHUB_RUN_ATTEMPT),
        testedCommit: process.env.GITHUB_SHA
      }
    : null;
  const report = `${JSON.stringify({
    schemaVersion: 1,
    scope: "historical-formal-release-backup-evidence",
    containsRealUserData: false,
    syntheticOnly: true,
    generatedAt: new Date().toISOString(),
    workflow,
    release: {
      repository: manifest.repository,
      releaseId: manifest.release.id,
      tag: manifest.release.tag,
      tagObjectSha: manifest.release.tagObjectSha,
      commit: manifest.release.commit,
      publishedAt: manifest.release.publishedAt,
      tagSigned: manifest.release.tagSigned
    },
    asset: {
      id: manifest.assets.automationZip.id,
      name: manifest.assets.automationZip.name,
      size: manifest.assets.automationZip.size,
      sha256: manifest.assets.automationZip.sha256,
      checksumAssetSha256: manifest.assets.automationChecksum.sha256,
      releaseChecksumsSha256: manifest.assets.releaseChecksums.sha256
    },
    binary: {
      name: path.basename(executable),
      size: manifest.archiveEntries.find((entry) => entry.name === path.basename(executable)).size,
      sha256: manifest.archiveEntries.find((entry) => entry.name === path.basename(executable)).sha256,
      authenticodeStatus: signatureStatus
    },
    backup: {
      metadataVersion: metadata.version,
      metadataSha256: sha256(metadataBytes),
      producedTreeSha256: producedBackupTreeSha256
    },
    verification: {
      releaseApiPinned: true,
      releaseChecksumPinned: true,
      isolatedEnvironment: true,
      authCanaryExcluded: true,
      currentNodeRestoreVerified: true,
      pendingRecoveryCount: status.pendingTransactions.length
    },
    limitation: "The formal v0.4.1 Automation binary is unsigned; provenance is bound to the hosted GitHub Release, fixed repository manifest, and SHA-256. Synthetic input is not real-user Beta evidence."
  }, null, 2)}\n`;
  assert.equal(report.includes(root), false);
  assert.equal(report.includes(fixture.authCanary), false);
  await fs.mkdir(path.dirname(reportPath), { recursive: true });
  await fs.writeFile(reportPath, report, "utf8");
});
