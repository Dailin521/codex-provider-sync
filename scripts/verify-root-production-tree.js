import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const forbidden = [
  "react",
  "react-dom",
  "vite",
  "typescript",
  "electron",
  "electron-vite",
  "electron-builder",
  "@codex-provider-sync/core",
  "@codex-provider-sync/contracts",
  "@codex-provider-sync/core-client"
];

for (const name of forbidden) {
  const candidate = path.join(repositoryRoot, "node_modules", ...name.split("/"));
  if (fs.existsSync(candidate)) {
    throw new Error(`Root production install contains forbidden dependency or workspace link: ${name}.`);
  }
}

process.stdout.write("Root production install contains no modern UI, Electron, TypeScript, or workspace links.\n");
