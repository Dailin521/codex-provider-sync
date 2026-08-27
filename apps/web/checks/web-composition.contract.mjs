import assert from "node:assert/strict";
import fs from "node:fs/promises";
import test from "node:test";

test("Web composition injects HttpCoreClient and keeps host APIs separate", async () => {
  const main = await fs.readFile(new URL("../src/main.tsx", import.meta.url), "utf8");
  const pairing = await fs.readFile(new URL("../src/pairing.ts", import.meta.url), "utf8");
  const html = await fs.readFile(new URL("../index.html", import.meta.url), "utf8");
  const themeBootstrap = await fs.readFile(new URL("../public/theme-bootstrap.js", import.meta.url), "utf8");
  assert.match(main, /new HttpCoreClient/);
  assert.match(main, /<AppUi/);
  assert.doesNotMatch(main, /\/api\/(?:status|backups|history|sync|switch|restore|prune)/);
  assert.match(pairing, /\/api\/pair/);
  assert.match(pairing, /\/api\/profiles/);
  assert.match(pairing, /history\.replaceState/);
  assert.doesNotMatch(pairing, /console\.|sessionStorage/);
  const storedKeys = [...pairing.matchAll(/localStorage\.(?:getItem|setItem|removeItem)\(([^)]+)/g)].map((match) => match[1]);
  assert.equal(storedKeys.every((key) => /DEVICE_STORAGE_KEY|LOCALE_STORAGE_KEY|THEME_STORAGE_KEY/.test(key)), true);
  assert.ok(html.indexOf("/theme-bootstrap.js") < html.indexOf("/src/main.tsx"));
  assert.match(themeBootstrap, /cps\.preference\.theme/);
  assert.match(themeBootstrap, /theme === "system" \|\| theme === "light" \|\| theme === "dark"/);
  assert.doesNotMatch(themeBootstrap, /eval|Function\s*\(|innerHTML|document\.write/);
});
