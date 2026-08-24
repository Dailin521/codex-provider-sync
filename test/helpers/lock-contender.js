import { createInterface } from "node:readline";

import { acquireLock } from "../../src/locking.js";

const BUSY_EXIT_CODE = 73;

function emit(event) {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

function isBusyError(error) {
  return error?.code === "OPERATION_BUSY"
    || error?.code === "TARGET_BUSY"
    || error?.message?.includes("Lock already exists") === true;
}

async function waitForRelease() {
  const lines = createInterface({ input: process.stdin, terminal: false });
  for await (const line of lines) {
    if (line.trim() === "release") {
      return;
    }
  }
}

async function main() {
  const [mode, codexHome] = process.argv.slice(2);
  if (!codexHome || (mode !== "hold" && mode !== "attempt")) {
    throw new Error("Usage: lock-contender.js <hold|attempt> <codex-home>");
  }

  const release = await acquireLock(codexHome, `cross-runtime-${mode}`);
  emit({ event: "acquired", pid: process.pid });

  if (mode === "hold") {
    await waitForRelease();
  }

  await release();
  emit({ event: "released", pid: process.pid });
}

try {
  await main();
} catch (error) {
  if (isBusyError(error)) {
    emit({
      event: "busy",
      code: error?.code ?? null,
      message: error?.message ?? String(error)
    });
    process.exitCode = BUSY_EXIT_CODE;
  } else {
    process.stderr.write(`${error?.stack ?? String(error)}\n`);
    process.exitCode = 1;
  }
}
