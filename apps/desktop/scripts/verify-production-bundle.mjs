import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const desktopRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outputRoot = path.join(desktopRoot, "out");

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
    .filter((file) => /\.(?:c?js|html)$/.test(file))
    .map((file) => fs.readFile(file, "utf8"))
)).join("\n");
for (const forbidden of [
  "__CPS_DESKTOP_TEST__",
  "CPS_DESKTOP_E2E",
  "CPS_DESKTOP_USER_DATA",
  "CPS_DESKTOP_TEST_GATE",
  "desktop E2E fault gate",
  "cps:v1:test:crash-runtime",
  "requestRaw",
  "crashRuntime"
]) {
  assert.doesNotMatch(processText, new RegExp(forbidden), `Production Electron output contains ${forbidden}.`);
}
assert.doesNotMatch(processText, /@codex-provider-sync\//, "Production Electron output contains a workspace import.");

const preload = await fs.readFile(path.join(outputRoot, "preload", "index.cjs"), "utf8");
assert.match(preload, /require\("electron"\)/);
assert.deepEqual(
  [...preload.matchAll(/require\("([^"]+)"\)/g)].map((match) => match[1]),
  ["electron"],
  "Sandbox preload requires something other than Electron."
);

process.stdout.write("Production Electron bundle boundary verified.\n");
