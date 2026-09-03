import fs from "node:fs";
import path from "node:path";

const expectedNode = "v16.20.2";

function installedNpmVersion() {
  const npmExecPath = process.env.npm_execpath;
  if (!npmExecPath) return null;
  let current = path.dirname(path.resolve(npmExecPath));
  for (let depth = 0; depth < 8; depth += 1) {
    const manifestPath = path.join(current, "package.json");
    try {
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      if (manifest.name === "npm" && typeof manifest.version === "string") return manifest.version;
    } catch {
      // Continue toward the filesystem root.
    }
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  return null;
}

const npmVersion = installedNpmVersion();
const npmMajor = npmVersion?.split(".")[0];

if (process.version !== expectedNode || npmMajor !== "8") {
  process.stderr.write(
    `Expected Node ${expectedNode} with npm 8; found Node ${process.version} and npm ${npmVersion ?? "unknown"}.\n`
  );
  process.exitCode = 1;
} else {
  process.stdout.write(`Verified Node ${process.version} with npm ${npmVersion}.\n`);
}
