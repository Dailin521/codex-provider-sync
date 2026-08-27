import { runRestore } from "../src/service.js";

const [codexHome, backupDir, crashPoint, ...flags] = process.argv.slice(2);
if (!codexHome || !backupDir || !crashPoint) {
  process.stderr.write(
    "usage: restore-v2-crash-host <codex-home> <backup-dir> <crash-point> "
      + "[--with-database] [--fail-at <fault-point>]\n"
  );
  process.exit(2);
}
const failureIndex = flags.indexOf("--fail-at");
const failurePoint = failureIndex >= 0 ? flags[failureIndex + 1] : null;
if (failureIndex >= 0 && !failurePoint) {
  process.stderr.write("--fail-at requires a fault point\n");
  process.exit(2);
}
let failureInjected = false;

await runRestore({
  codexHome,
  backupDir,
  restoreDatabase: flags.includes("--with-database"),
  faultInjector: ({ point }) => {
    if (point === failurePoint && !failureInjected) {
      failureInjected = true;
      throw new Error(`forced Restore failure at ${point}`);
    }
    if (point === crashPoint) {
      process.exit(86);
    }
  }
});

process.exit(0);
