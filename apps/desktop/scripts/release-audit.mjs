import assert from "node:assert/strict";
import crypto from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { extractFile, getRawHeader, listPackage } from "@electron/asar";
import {
  FuseState,
  FuseV1Options,
  getCurrentFuseWire
} from "@electron/fuses";
import { parse as parsePlist, parseBinary as parseBinaryPlist } from "plist";
import { NtExecutable, NtExecutableResource } from "resedit";

const require = createRequire(import.meta.url);
const scriptRoot = path.dirname(fileURLToPath(import.meta.url));
const desktopRoot = path.resolve(scriptRoot, "..");
const repositoryRoot = path.resolve(desktopRoot, "../..");
const nativeProbeScript = path.join(scriptRoot, "native-driver-probe.cjs");
export const ARTIFACT_AUDIT_POLICY_PATH = path.join(
  desktopRoot,
  "release",
  "artifact-audit-policy.v1.json"
);
const AUDIT_POLICY = JSON.parse(fsSync.readFileSync(ARTIFACT_AUDIT_POLICY_PATH, "utf8"));
assert.equal(AUDIT_POLICY.schemaVersion, 1, "Unsupported artifact audit policy.");
const REQUIRED_ASAR_ENTRIES = Object.freeze([...AUDIT_POLICY.requiredAsarEntries]);
const FORBIDDEN_SEGMENTS = new Set(AUDIT_POLICY.forbiddenPathSegments);
const FORBIDDEN_NAMES = new Set(AUDIT_POLICY.forbiddenFileNames);
const FORBIDDEN_EXTENSIONS = new Set(AUDIT_POLICY.forbiddenExtensions);
const FORBIDDEN_FILE_PATTERNS = Object.freeze(
  AUDIT_POLICY.forbiddenFilePatterns.map((pattern) => new RegExp(pattern))
);
const FORBIDDEN_TEXT_RULES = Object.freeze(
  AUDIT_POLICY.forbiddenTextRules.map((rule) => Object.freeze({
    id: rule.id,
    pattern: new RegExp(rule.pattern)
  }))
);

export const RELEASE_TARGETS = Object.freeze({
  "windows-x64": Object.freeze({
    platform: "win32",
    arch: "x64",
    nativeBinding: "node_modules/better-sqlite3/prebuilds/win32-x64.node",
    unpackedDirectories: ["win-unpacked"],
    assets(version) {
      return [
        `CodexProviderSync-${version}-windows-x64-setup.exe`,
        `CodexProviderSync-${version}-windows-x64-portable.zip`
      ];
    }
  }),
  "macos-x64": Object.freeze({
    platform: "darwin",
    arch: "x64",
    nativeBinding: "node_modules/better-sqlite3/prebuilds/darwin-x64.node",
    unpackedDirectories: ["mac", "mac-x64"],
    assets(version) {
      return [
        `CodexProviderSync-${version}-macos-x64.dmg`,
        `CodexProviderSync-${version}-macos-x64.zip`
      ];
    }
  }),
  "macos-arm64": Object.freeze({
    platform: "darwin",
    arch: "arm64",
    nativeBinding: "node_modules/better-sqlite3/prebuilds/darwin-arm64.node",
    unpackedDirectories: ["mac-arm64", "mac"],
    assets(version) {
      return [
        `CodexProviderSync-${version}-macos-arm64.dmg`,
        `CodexProviderSync-${version}-macos-arm64.zip`
      ];
    }
  }),
  "linux-x64": Object.freeze({
    platform: "linux",
    arch: "x64",
    nativeBinding: "node_modules/better-sqlite3/prebuilds/linux-x64.node",
    unpackedDirectories: ["linux-unpacked"],
    assets(version) {
      return [
        `CodexProviderSync-${version}-linux-x64.AppImage`,
        `CodexProviderSync-${version}-linux-x64.deb`
      ];
    }
  })
});

function normalizeEntry(entry) {
  return entry.replace(/^pack\s*:\s*/, "").replaceAll("\\", "/").replace(/^\/+/, "");
}

function asarEntryPath(entry) {
  return normalizeEntry(entry).split("/").join(path.sep);
}

function headerNode(header, entry) {
  let node = header;
  for (const segment of normalizeEntry(entry).split("/")) {
    node = node?.files?.[segment];
    if (!node) return null;
  }
  return node;
}

function bufferIntegrity(buffer, blockSize) {
  const blocks = [];
  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    blocks.push(crypto.createHash("sha256").update(buffer.subarray(offset, offset + blockSize)).digest("hex"));
  }
  if (buffer.length === 0) blocks.push(crypto.createHash("sha256").update(buffer).digest("hex"));
  return Object.freeze({
    hash: crypto.createHash("sha256").update(buffer).digest("hex"),
    blocks
  });
}

function verifyAsarEntryIntegrity(rawHeader, asarPath, entries) {
  let verified = 0;
  for (const entry of entries) {
    const node = headerNode(rawHeader.header, entry);
    if (!node || typeof node.size !== "number" || node.unpacked || node.link) continue;
    const integrity = node.integrity;
    assert.ok(integrity && typeof integrity === "object", `ASAR entry lacks integrity metadata: ${entry}`);
    assert.equal(integrity.algorithm, "SHA256", `ASAR entry uses an unexpected integrity algorithm: ${entry}`);
    assert.equal(Number.isSafeInteger(integrity.blockSize) && integrity.blockSize > 0, true);
    assert.equal(Array.isArray(integrity.blocks), true);
    const value = extractFile(asarPath, asarEntryPath(entry));
    const actual = bufferIntegrity(value, integrity.blockSize);
    assert.equal(actual.hash, integrity.hash, `ASAR entry hash mismatch: ${entry}`);
    assert.deepEqual(actual.blocks, integrity.blocks, `ASAR entry block hash mismatch: ${entry}`);
    verified += 1;
  }
  assert.ok(verified > 0, "ASAR contains no integrity-protected files.");
  return verified;
}

async function verifyEmbeddedAsarIntegrity(layout, descriptor, headerSha256) {
  if (descriptor.platform === "linux") return "unsupported-platform";
  if (descriptor.platform === "win32") {
    const executable = NtExecutable.from(await fs.readFile(layout.executable));
    const resources = NtExecutableResource.from(executable);
    const matches = resources.entries.filter((entry) =>
      String(entry.type).toUpperCase() === "INTEGRITY"
      && String(entry.id).toUpperCase() === "ELECTRONASAR");
    assert.equal(matches.length, 1, "Windows executable must contain one Electron ASAR integrity resource.");
    const records = JSON.parse(Buffer.from(matches[0].bin).toString("utf8"));
    assert.deepEqual(records, [{
      file: "resources\\app.asar",
      alg: "SHA256",
      value: headerSha256
    }], "Windows executable ASAR integrity binding is invalid.");
    return "verified";
  }

  const infoPath = path.join(layout.appRoot, "Contents", "Info.plist");
  const infoBuffer = await fs.readFile(infoPath);
  const info = infoBuffer.subarray(0, 8).toString("ascii") === "bplist00"
    ? parseBinaryPlist(infoBuffer)
    : parsePlist(infoBuffer);
  const integrity = info?.ElectronAsarIntegrity;
  assert.ok(integrity && typeof integrity === "object" && !Array.isArray(integrity));
  const records = Object.entries(integrity);
  assert.equal(records.length, 1, "macOS app must contain one Electron ASAR integrity binding.");
  const [resourcePath, record] = records[0];
  assert.equal(resourcePath.replaceAll("\\", "/"), "Resources/app.asar");
  assert.deepEqual(record, { algorithm: "SHA256", hash: headerSha256 });
  return "verified";
}

function isFileNotFound(error) {
  return error && typeof error === "object" && error.code === "ENOENT";
}

async function isDirectory(value) {
  try {
    return (await fs.stat(value)).isDirectory();
  } catch (error) {
    if (isFileNotFound(error)) return false;
    throw error;
  }
}

export async function sha256File(filePath) {
  const hash = crypto.createHash("sha256");
  const handle = await fs.open(filePath, "r");
  try {
    for await (const chunk of handle.createReadStream()) hash.update(chunk);
  } finally {
    await handle.close();
  }
  return hash.digest("hex");
}

export function assertSafeAsarEntries(entries) {
  for (const entry of entries) {
    const normalized = normalizeEntry(entry);
    const lower = normalized.toLowerCase();
    const segments = lower.split("/").filter(Boolean);
    const name = segments.at(-1) || "";
    const forbiddenNamePattern = FORBIDDEN_FILE_PATTERNS.some((pattern) => pattern.test(name));
    assert.equal(
      segments.some((segment) => FORBIDDEN_SEGMENTS.has(segment)),
      false,
      `Packaged ASAR contains forbidden path segment: ${normalized}`
    );
    assert.equal(FORBIDDEN_NAMES.has(name), false, `Packaged ASAR contains forbidden file: ${normalized}`);
    assert.equal(forbiddenNamePattern, false, `Packaged ASAR contains forbidden file pattern: ${normalized}`);
    assert.equal(
      FORBIDDEN_EXTENSIONS.has(path.posix.extname(name)),
      false,
      `Packaged ASAR contains forbidden extension: ${normalized}`
    );
  }
}

export function assertSafeProductTextEntry(entry, value) {
  for (const rule of FORBIDDEN_TEXT_RULES) {
    assert.equal(rule.pattern.test(value), false, `Packaged product text violates ${rule.id}: ${entry}`);
  }
}

async function filesUnder(root) {
  const result = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`Packaged unpacked tree contains a symbolic link: ${entry.name}`);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) result.push(absolute);
      else throw new Error(`Packaged unpacked tree contains an unsupported entry: ${entry.name}`);
    }
  }
  await visit(root);
  return result.sort((left, right) => left.localeCompare(right));
}

async function assertSafeContainerTree(root) {
  const resolvedRoot = path.resolve(root);
  const entries = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      const relative = path.relative(root, absolute).replaceAll("\\", "/");
      entries.push(relative);
      if (entry.isSymbolicLink()) {
        const target = await fs.readlink(absolute);
        assert.equal(path.isAbsolute(target), false, `Packaged app contains an absolute symbolic link: ${relative}`);
        const resolvedTarget = path.resolve(path.dirname(absolute), target);
        const targetRelative = path.relative(resolvedRoot, resolvedTarget);
        assert.equal(
          targetRelative !== "" && !targetRelative.startsWith("..") && !path.isAbsolute(targetRelative),
          true,
          `Packaged app symbolic link escapes its root: ${relative}`
        );
      } else if (entry.isDirectory()) await visit(absolute);
      else if (!entry.isFile()) throw new Error(`Packaged app contains an unsupported entry: ${relative}`);
    }
  }
  await visit(root);
  assertSafeAsarEntries(entries);
}

export async function resolvePackagedLayout(outputRoot, target) {
  const descriptor = RELEASE_TARGETS[target];
  if (!descriptor) throw new Error("Unknown release target.");
  for (const directory of descriptor.unpackedDirectories) {
    const unpackedRoot = path.join(outputRoot, directory);
    if (!await isDirectory(unpackedRoot)) continue;
    if (descriptor.platform === "darwin") {
      const appRoot = path.join(unpackedRoot, "Codex Provider Sync.app");
      if (!await isDirectory(appRoot)) continue;
      return Object.freeze({
        unpackedRoot,
        appRoot,
        resources: path.join(appRoot, "Contents", "Resources"),
        executable: path.join(appRoot, "Contents", "MacOS", "Codex Provider Sync")
      });
    }
    return Object.freeze({
      unpackedRoot,
      appRoot: unpackedRoot,
      resources: path.join(unpackedRoot, "resources"),
      executable: descriptor.platform === "win32"
        ? path.join(unpackedRoot, "Codex Provider Sync.exe")
        : path.join(unpackedRoot, "codex-provider-sync")
    });
  }
  throw new Error(`No unpacked ${target} application was found under the builder output.`);
}

function parsePackageJson(buffer, label) {
  try {
    return JSON.parse(buffer.toString("utf8"));
  } catch {
    throw new Error(`Packaged ${label} is not valid JSON.`);
  }
}

function componentLicense(value) {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (value && typeof value === "object" && typeof value.type === "string") return value.type;
  return null;
}

function packagedComponents(asarPath, entries) {
  const result = new Map();
  for (const entry of entries) {
    if (entry !== "package.json"
        && !/(?:^|\/)node_modules\/(?:@[^/]+\/[^/]+|[^/]+)\/package\.json$/.test(entry)) continue;
    let manifest;
    try {
      manifest = parsePackageJson(extractFile(asarPath, asarEntryPath(entry)), entry);
    } catch {
      continue;
    }
    if (typeof manifest.name !== "string" || typeof manifest.version !== "string") continue;
    const key = `${manifest.name}@${manifest.version}`;
    if (!result.has(key)) {
      result.set(key, Object.freeze({
        name: manifest.name,
        version: manifest.version,
        license: componentLicense(manifest.license)
      }));
    }
  }
  return [...result.values()].sort((left, right) => `${left.name}@${left.version}`.localeCompare(`${right.name}@${right.version}`));
}

function fuseIs(fuses, option, expected) {
  assert.equal(fuses[option], expected, `Electron fuse ${FuseV1Options[option]} has an unsafe state.`);
}

async function verifyNativeDriver(nativeBinding, asarPath) {
  const electronBinary = require("electron");
  const sourcePackage = path.join(asarPath, "node_modules", "better-sqlite3");
  const args = process.platform === "linux" && typeof process.getuid === "function" && process.getuid() === 0
    ? ["--no-sandbox", nativeProbeScript]
    : [nativeProbeScript];
  const result = spawnSync(electronBinary, args, {
    cwd: desktopRoot,
    env: {
      ...process.env,
      CPS_NATIVE_DRIVER_PACKAGE: sourcePackage,
      CPS_NATIVE_DRIVER_BINDING: nativeBinding
    },
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`Electron native driver probe failed with exit code ${result.status}: ${String(result.stderr).trim()}`);
  }
  const match = String(result.stdout).match(/CPS_NATIVE_DRIVER_RESULT=(\{[^\r\n]+\})/);
  if (!match) throw new Error("Electron native driver probe did not return a result.");
  const probe = JSON.parse(match[1]);
  assert.equal(probe.driver, "better-sqlite3");
  assert.equal(probe.electron, "44.0.0");
  assert.match(String(probe.modules), /^\d+$/);
  return Object.freeze({
    driver: probe.driver,
    electron: probe.electron,
    modules: String(probe.modules),
    sqlite: typeof probe.sqlite === "string" ? probe.sqlite : null
  });
}

export async function auditPackagedLayout({ layout, target, version, buildId }) {
  const descriptor = RELEASE_TARGETS[target];
  if (!descriptor) throw new Error("Unknown release target.");
  const asarPath = path.join(layout.resources, "app.asar");
  const unpackedPath = `${asarPath}.unpacked`;
  const [asarStat, executableStat] = await Promise.all([fs.stat(asarPath), fs.stat(layout.executable)]);
  assert.equal(asarStat.isFile(), true, "Packaged app.asar is missing.");
  assert.equal(executableStat.isFile(), true, "Packaged executable is missing.");
  await assertSafeContainerTree(layout.appRoot);

  const entries = listPackage(asarPath, { isPack: false }).map(normalizeEntry);
  const entrySet = new Set(entries);
  for (const required of REQUIRED_ASAR_ENTRIES) {
    assert.equal(entrySet.has(required), true, `Packaged ASAR is missing ${required}.`);
  }
  assertSafeAsarEntries(entries);
  const nativePrebuildEntries = entries.filter((entry) =>
    /^node_modules\/better-sqlite3\/prebuilds\/[^/]+\.node$/.test(entry));
  assert.deepEqual(
    nativePrebuildEntries,
    [descriptor.nativeBinding],
    "Packaged ASAR must reference only the target platform's native SQLite binding."
  );
  assert.equal(
    headerNode(getRawHeader(asarPath).header, descriptor.nativeBinding)?.unpacked,
    true,
    "The target native SQLite binding must be marked unpacked."
  );

  const manifest = parsePackageJson(extractFile(asarPath, asarEntryPath("package.json")), "package.json");
  assert.equal(manifest.name, "@codex-provider-sync/desktop");
  assert.equal(manifest.version, version, "Packaged Electron version does not match the candidate.");
  assert.equal(manifest.main, "out/main/index.js");

  const productTextEntries = entries.filter((entry) => entry.startsWith("out/") && /\.(?:c?js|mjs|html|css|json)$/.test(entry));
  let buildIdFound = false;
  for (const entry of productTextEntries) {
    const value = extractFile(asarPath, asarEntryPath(entry)).toString("utf8");
    if (value.includes(buildId)) buildIdFound = true;
    assertSafeProductTextEntry(entry, value);
  }
  assert.equal(buildIdFound, true, "Packaged build ID is missing.");

  const unpackedFiles = await filesUnder(unpackedPath);
  const unpackedRelative = unpackedFiles.map((file) => path.relative(unpackedPath, file).replaceAll("\\", "/"));
  assert.deepEqual(
    unpackedRelative,
    [descriptor.nativeBinding],
    "Only the target platform's native SQLite binding may be outside app.asar."
  );

  const fuses = await getCurrentFuseWire(layout.executable);
  fuseIs(fuses, FuseV1Options.RunAsNode, FuseState.DISABLE);
  fuseIs(fuses, FuseV1Options.EnableCookieEncryption, FuseState.ENABLE);
  fuseIs(fuses, FuseV1Options.EnableNodeOptionsEnvironmentVariable, FuseState.DISABLE);
  fuseIs(fuses, FuseV1Options.EnableNodeCliInspectArguments, FuseState.DISABLE);
  fuseIs(fuses, FuseV1Options.EnableEmbeddedAsarIntegrityValidation, FuseState.ENABLE);
  fuseIs(fuses, FuseV1Options.OnlyLoadAppFromAsar, FuseState.ENABLE);
  fuseIs(fuses, FuseV1Options.LoadBrowserProcessSpecificV8Snapshot, FuseState.DISABLE);
  fuseIs(fuses, FuseV1Options.GrantFileProtocolExtraPrivileges, FuseState.DISABLE);

  const nativeBinding = unpackedFiles[0];
  const nativeProbe = await verifyNativeDriver(nativeBinding, asarPath);
  const rawHeader = getRawHeader(asarPath);
  const headerSha256 = crypto.createHash("sha256").update(rawHeader.headerString).digest("hex");
  const integrityEntryCount = verifyAsarEntryIntegrity(rawHeader, asarPath, entries);
  const runtimeIntegrity = await verifyEmbeddedAsarIntegrity(layout, descriptor, headerSha256);
  const components = packagedComponents(asarPath, entries);
  return Object.freeze({
    schemaVersion: 1,
    target,
    platform: descriptor.platform,
    arch: descriptor.arch,
    version,
    buildId,
    asar: Object.freeze({
      sha256: await sha256File(asarPath),
      headerSha256,
      entryCount: entries.length,
      integrityEntryCount,
      runtimeIntegrity
    }),
    nativeDriver: Object.freeze({
      ...nativeProbe,
      bindingSha256: await sha256File(nativeBinding)
    }),
    fuses: Object.freeze({
      runAsNode: false,
      cookieEncryption: true,
      nodeOptions: false,
      nodeCliInspect: false,
      embeddedAsarIntegrity: true,
      onlyLoadAppFromAsar: true,
      browserProcessV8Snapshot: false,
      fileProtocolExtraPrivileges: false
    }),
    components
  });
}

export async function auditPackagedApp({ outputRoot, target, version, buildId }) {
  const layout = await resolvePackagedLayout(outputRoot, target);
  return auditPackagedLayout({ layout, target, version, buildId });
}

export async function auditExtractedApp({ appRoot, target, version, buildId }) {
  const descriptor = RELEASE_TARGETS[target];
  if (!descriptor) throw new Error("Unknown release target.");
  const resolvedRoot = path.resolve(appRoot);
  const layout = descriptor.platform === "darwin"
    ? Object.freeze({
      unpackedRoot: path.dirname(resolvedRoot),
      appRoot: resolvedRoot,
      resources: path.join(resolvedRoot, "Contents", "Resources"),
      executable: path.join(resolvedRoot, "Contents", "MacOS", "Codex Provider Sync")
    })
    : Object.freeze({
      unpackedRoot: resolvedRoot,
      appRoot: resolvedRoot,
      resources: path.join(resolvedRoot, "resources"),
      executable: path.join(
        resolvedRoot,
        descriptor.platform === "win32" ? "Codex Provider Sync.exe" : "codex-provider-sync"
      )
    });
  return auditPackagedLayout({ layout, target, version, buildId });
}

function resolveLockDependency(packages, fromKey, dependencyName) {
  let current = fromKey;
  while (true) {
    const candidate = path.posix.join(current, "node_modules", dependencyName);
    const entry = packages[candidate];
    if (entry) return entry.link ? entry.resolved : candidate;
    if (!current) break;
    const parent = path.posix.dirname(current);
    current = parent === "." ? "" : parent;
  }
  return null;
}

function npmPurl(name, version) {
  if (name.startsWith("@")) {
    const [scope, packageName] = name.slice(1).split("/");
    return `pkg:npm/%40${encodeURIComponent(scope)}/${encodeURIComponent(packageName)}@${encodeURIComponent(version)}`;
  }
  return `pkg:npm/${encodeURIComponent(name)}@${encodeURIComponent(version)}`;
}

function integrityHash(integrity) {
  const match = typeof integrity === "string" ? integrity.match(/^sha512-(.+)$/) : null;
  if (!match) return [];
  return [{ alg: "SHA-512", content: Buffer.from(match[1], "base64").toString("hex") }];
}

export async function createRuntimeProjection(lockfilePath) {
  const lockfile = JSON.parse(await fs.readFile(lockfilePath, "utf8"));
  assert.equal(lockfile.lockfileVersion, 3, "C9 runtime projection requires npm lockfile v3.");
  const packages = lockfile.packages;
  const rootKey = "apps/desktop";
  assert.equal(packages[rootKey]?.name, "@codex-provider-sync/desktop");
  const records = new Map();
  const visited = new Set();

  function visit(lockKey, expectedName) {
    if (visited.has(lockKey)) {
      const existing = packages[lockKey];
      return `${existing.name ?? expectedName}@${existing.version}`;
    }
    visited.add(lockKey);
    const entry = packages[lockKey];
    assert.ok(entry && typeof entry === "object", `Runtime dependency is absent from lockfile: ${lockKey}`);
    const name = entry.name ?? expectedName;
    assert.equal(typeof name, "string");
    assert.equal(typeof entry.version, "string");
    const ref = `${name}@${entry.version}`;
    const dependencyRefs = new Set();
    const dependencyGroups = [
      [entry.dependencies ?? {}, false],
      [entry.optionalDependencies ?? {}, true],
      [entry.peerDependencies ?? {}, false]
    ];
    for (const [dependencies, optionalGroup] of dependencyGroups) {
      for (const dependencyName of Object.keys(dependencies).sort()) {
        const optionalPeer = entry.peerDependenciesMeta?.[dependencyName]?.optional === true;
        const dependencyKey = resolveLockDependency(packages, lockKey, dependencyName);
        if (!dependencyKey) {
          if (optionalGroup || optionalPeer) continue;
          throw new Error(`Required runtime dependency is unresolved: ${name} -> ${dependencyName}`);
        }
        dependencyRefs.add(visit(dependencyKey, dependencyName));
      }
    }
    const current = records.get(ref);
    if (current) {
      for (const dependencyRef of dependencyRefs) current.dependencies.add(dependencyRef);
    } else {
      records.set(ref, {
        ref,
        name,
        version: entry.version,
        license: typeof entry.license === "string" ? entry.license : null,
        resolved: typeof entry.resolved === "string" && /^https:\/\//.test(entry.resolved) ? entry.resolved : null,
        hashes: integrityHash(entry.integrity),
        dependencies: dependencyRefs
      });
    }
    return ref;
  }

  const rootRef = visit(rootKey, "@codex-provider-sync/desktop");
  const root = records.get(rootRef);
  records.delete(rootRef);
  return Object.freeze({
    rootDependencies: [...root.dependencies].sort(),
    components: [...records.values()]
      .map((record) => Object.freeze({ ...record, dependencies: [...record.dependencies].sort() }))
      .sort((left, right) => left.ref.localeCompare(right.ref))
  });
}

export function createCycloneDx({ audit, timestamp, runtimeProjection }) {
  const applicationRef = `application:@codex-provider-sync/desktop@${audit.version}`;
  const electronRef = "framework:electron@44.0.0";
  const projected = new Set(runtimeProjection.components.map((component) => component.ref));
  for (const component of audit.components) {
    if (component.name === "@codex-provider-sync/desktop") continue;
    assert.equal(
      projected.has(`${component.name}@${component.version}`),
      true,
      `Packaged component is absent from the runtime lock projection: ${component.name}@${component.version}`
    );
  }
  const libraries = runtimeProjection.components.map((component) => ({
      type: "library",
      name: component.name,
      version: component.version,
      "bom-ref": `library:${component.name}@${component.version}`,
      purl: npmPurl(component.name, component.version),
      ...(component.license ? { licenses: [{ license: { name: component.license } }] } : {}),
      ...(component.hashes.length ? { hashes: component.hashes } : {}),
      ...(component.resolved ? {
        externalReferences: [{ type: "distribution", url: component.resolved }]
      } : {})
    }));
  const libraryRefs = new Map(libraries.map((component) => [
    `${component.name}@${component.version}`,
    component["bom-ref"]
  ]));
  return {
    bomFormat: "CycloneDX",
    specVersion: "1.6",
    serialNumber: `urn:uuid:${crypto.randomUUID()}`,
    version: 1,
    metadata: {
      timestamp,
      tools: {
        components: [{ type: "application", name: "codex-provider-sync-c9-auditor", version: "1" }]
      },
      component: {
        type: "application",
        name: "@codex-provider-sync/desktop",
        version: audit.version,
        "bom-ref": applicationRef
      }
    },
    components: [
      { type: "framework", name: "electron", version: "44.0.0", "bom-ref": electronRef },
      ...libraries
    ],
    dependencies: [
      {
        ref: applicationRef,
        dependsOn: [
          electronRef,
          ...runtimeProjection.rootDependencies.map((ref) => libraryRefs.get(ref))
        ].sort()
      },
      { ref: electronRef, dependsOn: [] },
      ...runtimeProjection.components.map((component) => ({
        ref: libraryRefs.get(component.ref),
        dependsOn: component.dependencies.map((ref) => libraryRefs.get(ref)).sort()
      }))
    ]
  };
}

export const RELEASE_REPOSITORY_ROOT = repositoryRoot;
