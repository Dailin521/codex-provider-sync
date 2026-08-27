import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outputRoot = path.join(desktopRoot, "out");
const auditPolicy = JSON.parse(await fs.readFile(
  path.join(desktopRoot, "release", "artifact-audit-policy.v1.json"),
  "utf8"
));
assert.equal(auditPolicy.schemaVersion, 1, "Unsupported artifact audit policy.");
const auditedTextExtensions = new Set(auditPolicy.auditedProductTextExtensions);
const forbiddenTextRules = auditPolicy.forbiddenTextRules.map((rule) => ({
  id: rule.id,
  pattern: new RegExp(rule.pattern)
}));

async function filesUnder(root) {
  const result = [];
  async function visit(directory) {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const absolute = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(absolute);
      else if (entry.isFile()) result.push(absolute);
    }
  }
  await visit(root);
  return result;
}

const files = await filesUnder(outputRoot);
const relative = files.map((file) => path.relative(outputRoot, file).replaceAll("\\", "/"));
for (const required of ["main/index.js", "main/runtime.js", "preload/index.cjs", "renderer/index.html"]) {
  assert.ok(relative.includes(required), `Production Electron output is missing ${required}.`);
}
assert.equal(relative.some((file) => file.endsWith(".map")), false, "Production Electron output contains source maps.");

const processText = (await Promise.all(
  files
    .filter((file) => auditedTextExtensions.has(path.extname(file).toLowerCase()))
    .map((file) => fs.readFile(file, "utf8"))
)).join("\n");
for (const rule of forbiddenTextRules) {
  assert.doesNotMatch(processText, rule.pattern, `Production Electron output violates ${rule.id}.`);
}
assert.doesNotMatch(processText, /@codex-provider-sync\//, "Production Electron output contains a workspace import.");

const preload = await fs.readFile(path.join(outputRoot, "preload", "index.cjs"), "utf8");
assert.match(preload, /require\("electron"\)/);
assert.deepEqual(
  [...preload.matchAll(/require\("([^"]+)"\)/g)].map((match) => match[1]),
  ["electron"],
  "Sandbox preload requires something other than Electron."
);

const main = await fs.readFile(path.join(outputRoot, "main", "index.js"), "utf8");
const runtime = await fs.readFile(path.join(outputRoot, "main", "runtime.js"), "utf8");
const renderer = (await Promise.all(
  files
    .filter((file) => path.relative(outputRoot, file).replaceAll("\\", "/").startsWith("renderer/")
      && /\.(?:js|html)$/.test(file))
    .map((file) => fs.readFile(file, "utf8"))
)).join("\n");
assert.match(main, /electron-updater/, "Production Main is missing the controlled updater.");
assert.doesNotMatch(preload, /electron-updater|autoUpdater|quitAndInstall/);
assert.doesNotMatch(runtime, /electron-updater|autoUpdater|quitAndInstall/);
assert.doesNotMatch(renderer, /electron-updater|autoUpdater|quitAndInstall|setFeedURL/);

process.stdout.write("Production Electron bundle boundary verified.\n");
