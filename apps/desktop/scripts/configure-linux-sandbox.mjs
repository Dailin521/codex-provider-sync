import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { constants as fsConstants } from "node:fs";

function fail(message) {
  throw new Error(`Linux Electron sandbox setup refused: ${message}`);
}

function isWithin(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative !== ""
    && !path.isAbsolute(relative)
    && relative !== ".."
    && !relative.startsWith(`..${path.sep}`);
}

if (process.platform !== "linux" || typeof process.getuid !== "function" || process.getuid() !== 0) {
  fail("the helper must run as root on Linux");
}

const [rootArgument, relativeArgument, ...extraArguments] = process.argv.slice(2);
if (!rootArgument || relativeArgument !== "chrome-sandbox" || extraArguments.length > 0) {
  fail("expected one allowed root and the exact chrome-sandbox filename");
}

const lexicalRoot = path.resolve(rootArgument);
const rootInfo = await fs.lstat(lexicalRoot, { bigint: true });
if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) {
  fail("the allowed root must be a real directory");
}
const realRoot = path.resolve(await fs.realpath(lexicalRoot));
if (realRoot !== lexicalRoot) {
  fail("the allowed root must already be canonical");
}

const sandboxPath = path.resolve(lexicalRoot, relativeArgument);
if (!isWithin(realRoot, sandboxPath)) {
  fail("the target escaped its allowed root");
}
const before = await fs.lstat(sandboxPath, { bigint: true });
if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1n) {
  fail("the target must be one regular, unlinked file");
}
const realSandboxPath = path.resolve(await fs.realpath(sandboxPath));
if (realSandboxPath !== sandboxPath || !isWithin(realRoot, realSandboxPath)) {
  fail("the target physical path escaped its allowed root");
}

const noFollow = fsConstants.O_NOFOLLOW;
if (typeof noFollow !== "number") fail("O_NOFOLLOW is unavailable");
const handle = await fs.open(sandboxPath, fsConstants.O_RDONLY | noFollow);
try {
  const opened = await handle.stat({ bigint: true });
  if (!opened.isFile()
      || opened.nlink !== 1n
      || opened.dev !== before.dev
      || opened.ino !== before.ino) {
    fail("the target changed before it could be opened safely");
  }
  await handle.chown(0, 0);
  await handle.chmod(0o4755);
  const configured = await handle.stat({ bigint: true });
  if (configured.uid !== 0n
      || (configured.mode & 0o7777n) !== 0o4755n
      || configured.nlink !== 1n) {
    fail("the opened target failed owner/mode verification");
  }
  const named = await fs.lstat(sandboxPath, { bigint: true });
  if (!named.isFile()
      || named.isSymbolicLink()
      || named.dev !== configured.dev
      || named.ino !== configured.ino
      || named.uid !== 0n
      || (named.mode & 0o7777n) !== 0o4755n
      || named.nlink !== 1n) {
    fail("the named target changed during owner/mode configuration");
  }
} finally {
  await handle.close();
}

process.stdout.write("Linux Electron sandbox owner and mode verified.\n");
