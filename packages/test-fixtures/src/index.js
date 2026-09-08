import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export const FIXTURE_SCHEMA_VERSION = 1;
export const DIFFERENCE_SCHEMA_VERSION = 1;

const FORBIDDEN_NAMES = new Set(["auth.json", ".env"]);
const SENSITIVE_FILE_NAME = /(^|[._-])(auth|credentials?|tokens?|secrets?|api[._-]?keys?|access[._-]?keys?|private[._-]?keys?|keys?|passwords?|passwds?|cookies?)([._-]|$)/i;
const SENSITIVE_NORMALIZED_FRAGMENTS = [
  "authorization",
  "credential",
  "password",
  "passwd",
  "messagebody",
  "message",
  "secret",
  "token",
  "cookie",
  "apikey",
  "accesskey",
  "privatekey"
];
const SENSITIVE_MANIFEST_KEYS = new Set([
  "accesskey",
  "accesskeys",
  "apikey",
  "apikeys",
  "auth",
  "authorization",
  "body",
  "cookie",
  "cookies",
  "credential",
  "credentials",
  "key",
  "keys",
  "message",
  "messagebody",
  "messages",
  "password",
  "passwords",
  "passwd",
  "privatekey",
  "privatekeys",
  "secret",
  "secrets",
  "token",
  "tokens"
]);
const FIXTURE_ID_PATTERN = /^[a-z0-9][a-z0-9._-]{0,79}$/;

function record(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value
    : null;
}

function relativeFixturePath(value, field) {
  if (typeof value !== "string" || !value || value.includes("\0") || path.isAbsolute(value)) {
    throw new TypeError(`${field} must be a non-empty relative path.`);
  }
  const normalized = path.normalize(value);
  if (normalized === ".." || normalized.startsWith(`..${path.sep}`)) {
    throw new TypeError(`${field} must stay inside the fixture root.`);
  }
  return normalized;
}

function exactKeys(value, allowed) {
  return Object.keys(value).every((key) => allowed.has(key));
}

function normalizedSensitiveName(value) {
  const normalized = value.toLowerCase().replaceAll(/[^a-z]/g, "");
  return SENSITIVE_MANIFEST_KEYS.has(normalized)
    || SENSITIVE_NORMALIZED_FRAGMENTS.some((fragment) => normalized.includes(fragment));
}

function assertNoSensitiveManifestFields(value, currentPath = "manifest", depth = 0) {
  if (depth > 16) throw new TypeError("Fixture manifest is too deeply nested.");
  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertNoSensitiveManifestFields(entry, `${currentPath}[${index}]`, depth + 1));
    return;
  }
  const object = record(value);
  if (!object) return;
  for (const [key, entry] of Object.entries(object)) {
    if (normalizedSensitiveName(key)) {
      throw new TypeError(`Fixture manifest cannot contain sensitive field ${currentPath}.${key}.`);
    }
    assertNoSensitiveManifestFields(entry, `${currentPath}.${key}`, depth + 1);
  }
}

export function validateFixtureManifest(value) {
  const manifest = record(value);
  if (!manifest
      || !exactKeys(manifest, new Set([
        "schemaVersion",
        "id",
        "description",
        "containsRealUserData",
        "inputs",
        "expected"
      ]))
      || manifest.schemaVersion !== FIXTURE_SCHEMA_VERSION
      || typeof manifest.id !== "string"
      || !FIXTURE_ID_PATTERN.test(manifest.id)
      || typeof manifest.description !== "string"
      || !manifest.description.trim()
      || manifest.containsRealUserData !== false) {
    throw new TypeError("Invalid or unsafe fixture manifest.");
  }
  const inputs = record(manifest.inputs);
  if (!inputs
      || !exactKeys(inputs, new Set(["codexHome", "sqliteHome"]))) {
    throw new TypeError("Fixture manifest inputs are invalid.");
  }
  const expected = record(manifest.expected);
  if (!expected) throw new TypeError("Fixture manifest expected is required.");
  assertNoSensitiveManifestFields(expected, "manifest.expected");
  const normalizedInputs = {
    codexHome: relativeFixturePath(inputs.codexHome, "inputs.codexHome"),
    ...(inputs.sqliteHome === undefined
      ? {}
      : { sqliteHome: relativeFixturePath(inputs.sqliteHome, "inputs.sqliteHome") })
  };
  return Object.freeze({
    schemaVersion: FIXTURE_SCHEMA_VERSION,
    id: manifest.id,
    description: manifest.description,
    containsRealUserData: false,
    inputs: Object.freeze(normalizedInputs),
    expected: Object.freeze({ ...expected })
  });
}

async function assertFixtureRootSafe(root) {
  const stat = await fs.lstat(root);
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new TypeError("Fixture root must be a real directory, not a symbolic link or reparse point.");
  }
}

async function assertFixtureTreeSafe(current) {
  const entries = await fs.readdir(current, { withFileTypes: true });
  for (const entry of entries) {
    const entryPath = path.join(current, entry.name);
    const stat = await fs.lstat(entryPath);
    if (stat.isSymbolicLink()) {
      throw new TypeError(`Fixture tree cannot contain symbolic links: ${entry.name}`);
    }
    if (FORBIDDEN_NAMES.has(entry.name.toLowerCase())
        || entry.name.toLowerCase().startsWith(".env.")
        || SENSITIVE_FILE_NAME.test(entry.name)
        || normalizedSensitiveName(path.parse(entry.name).name)) {
      throw new TypeError(`Fixture tree contains a forbidden file: ${entry.name}`);
    }
    if (stat.isDirectory()) await assertFixtureTreeSafe(entryPath);
    else if (!stat.isFile()) {
      throw new TypeError(`Fixture tree contains an unsupported filesystem entry: ${entry.name}`);
    }
  }
}

function pathIsWithin(root, target) {
  const normalizedRoot = process.platform === "win32" ? root.toLowerCase() : root;
  const normalizedTarget = process.platform === "win32" ? target.toLowerCase() : target;
  const relative = path.relative(normalizedRoot, normalizedTarget);
  return relative === "" || (relative !== ".."
    && !relative.startsWith(`..${path.sep}`)
    && !path.isAbsolute(relative));
}

async function assertFixtureInput(root, relativePath, field) {
  const target = path.join(root, relativePath);
  const [canonicalRoot, canonicalTarget, stat] = await Promise.all([
    fs.realpath(root),
    fs.realpath(target),
    fs.stat(target)
  ]);
  if (!pathIsWithin(canonicalRoot, canonicalTarget) || !stat.isDirectory()) {
    throw new TypeError(`${field} must resolve to a directory inside the fixture root.`);
  }
}

export async function readFixtureManifest(fixtureRoot) {
  const absoluteRoot = path.resolve(fixtureRoot);
  await assertFixtureRootSafe(absoluteRoot);
  const manifest = JSON.parse(await fs.readFile(path.join(absoluteRoot, "fixture.json"), "utf8"));
  const validated = validateFixtureManifest(manifest);
  await assertFixtureTreeSafe(absoluteRoot);
  await assertFixtureInput(absoluteRoot, validated.inputs.codexHome, "inputs.codexHome");
  if (validated.inputs.sqliteHome) {
    await assertFixtureInput(absoluteRoot, validated.inputs.sqliteHome, "inputs.sqliteHome");
  }
  return validated;
}

export async function runFixtureInTemp(fixtureRoot, run, { tempParent = os.tmpdir() } = {}) {
  if (typeof run !== "function") throw new TypeError("Fixture runner callback is required.");
  const sourceRoot = path.resolve(fixtureRoot);
  const manifest = await readFixtureManifest(sourceRoot);
  const tempRoot = await fs.mkdtemp(path.join(path.resolve(tempParent), "codex-provider-sync-fixture-"));
  const stagedRoot = path.join(tempRoot, "fixture");
  try {
    await fs.cp(sourceRoot, stagedRoot, {
      recursive: true,
      force: false,
      errorOnExist: true,
      dereference: false
    });
    const stagedManifest = await readFixtureManifest(stagedRoot);
    if (JSON.stringify(stagedManifest) !== JSON.stringify(manifest)) {
      throw new TypeError("Fixture manifest changed while it was being staged.");
    }
    return await run({
      root: stagedRoot,
      codexHome: path.join(stagedRoot, stagedManifest.inputs.codexHome),
      sqliteHome: stagedManifest.inputs.sqliteHome
        ? path.join(stagedRoot, stagedManifest.inputs.sqliteHome)
        : null,
      manifest: stagedManifest
    });
  } finally {
    await fs.rm(tempRoot, { recursive: true, force: true });
  }
}

export function createRuntimeDifference({
  fixtureId,
  status,
  node,
  dotnet,
  decision,
  notes = []
}) {
  if (typeof fixtureId !== "string" || !fixtureId) throw new TypeError("fixtureId is required.");
  if (!["matched", "accepted", "blocked"].includes(status)) {
    throw new TypeError("Difference status must be matched, accepted, or blocked.");
  }
  if (!Array.isArray(notes) || notes.some((entry) => typeof entry !== "string")) {
    throw new TypeError("Difference notes must be strings.");
  }
  return {
    schemaVersion: DIFFERENCE_SCHEMA_VERSION,
    fixtureId,
    status,
    node: record(node) ?? {},
    dotnet: record(dotnet) ?? {},
    decision: typeof decision === "string" ? decision : "",
    notes: [...notes]
  };
}
