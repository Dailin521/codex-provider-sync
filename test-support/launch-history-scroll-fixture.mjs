// Manual CLI/CDP fixture harness. Never uses or modifies a real Codex Home.
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import path from "node:path";
import { createDesktopReadOnlyFixture } from "./desktop-readonly-fixture.mjs";
import { claimDailyUpdateCheck } from "../apps/desktop/dist/main/daily-update-check.js";

const require = createRequire(new URL("../apps/desktop/package.json", import.meta.url));
const executable = process.env.CPS_DESKTOP_EXECUTABLE || require("electron");
const fixture = await createDesktopReadOnlyFixture({ scrollStress: true, includeUntitled: true });
await claimDailyUpdateCheck(fixture.userData); // Fixture interaction must not trigger a real update request.
const child = spawn(executable, [
  ...(process.env.CPS_DESKTOP_EXECUTABLE ? [] : [path.resolve("apps/desktop/out/main/index.js")]),
  `--user-data-dir=${fixture.userData}`, "--lang=en-US", "--remote-debugging-port=0"
], {
  env: { ...process.env, CODEX_HOME: fixture.codexHome, CPS_DESKTOP_WINDOW_DISPLAY: process.env.CPS_DESKTOP_WINDOW_DISPLAY === "secondary" ? "secondary" : "hidden" },
  windowsHide: true, stdio: ["ignore", "pipe", "pipe"]
});
let reported = false;
const report = (chunk) => {
  const endpoint = String(chunk).match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1];
  if (endpoint && !reported) { reported = true; process.stdout.write(`HISTORY_SCROLL_CDP=${endpoint}\n`); }
};
child.stdout.on("data", report);
child.stderr.on("data", report);
process.stdin.resume();
process.stdin.on("data", () => child.kill());
try {
  await new Promise((resolve, reject) => { child.once("exit", resolve); child.once("error", reject); });
  await fixture.assertUnchanged();
  process.stdout.write("History scroll fixture unchanged.\n");
} finally {
  await fixture.close();
  process.stdin.pause();
}
