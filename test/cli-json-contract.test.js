import { spawn } from "node:child_process";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { runCli } from "../src/cli.js";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cliPath = path.join(repositoryRoot, "src", "cli.js");
const driverPath = path.join(repositoryRoot, "test-support", "cli-json-driver.js");
const ENVELOPE_KEYS = [
  "schemaVersion",
  "command",
  "ok",
  "outcome",
  "result",
  "warnings",
  "error"
];

async function runNode(scriptPath, args, { scenario, env = {} } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [scriptPath, ...args], {
      cwd: repositoryRoot,
      env: {
        ...process.env,
        ...env,
        ...(scenario ? { CODEX_PROVIDER_SYNC_CLI_SCENARIO: scenario } : {})
      },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

function parseSingleEnvelope(stdout) {
  assert.ok(stdout.endsWith("\n"), "stdout must end with exactly one JSON line terminator");
  assert.equal((stdout.match(/\n/g) ?? []).length, 1, stdout);
  const envelope = JSON.parse(stdout);
  assert.deepEqual(Object.keys(envelope), ENVELOPE_KEYS);
  assert.equal(envelope.schemaVersion, 1);
  assert.equal(/\x1b\[/.test(stdout), false, "stdout must not contain ANSI sequences");
  return envelope;
}

test("real CLI emits schema help JSON and keeps Human help unchanged", async () => {
  const json = await runNode(cliPath, ["--json"]);
  assert.equal(json.code, 0);
  assert.equal(json.signal, null);
  assert.equal(json.stderr, "");
  const envelope = parseSingleEnvelope(json.stdout);
  assert.equal(envelope.command, "help");
  assert.equal(envelope.ok, true);
  assert.match(envelope.result.text, /^codex-provider\n\nUsage:/);

  const human = await runNode(cliPath, ["--help"]);
  assert.equal(human.code, 0);
  assert.equal(human.stderr, "");
  assert.match(human.stdout, /^codex-provider\r?\n\r?\nUsage:/);
  assert.doesNotMatch(human.stdout, /"schemaVersion"/);
});

test("real CLI JSON input failures use one stdout document and exit 2", async () => {
  const cases = [
    ["unknown", "--json"],
    ["sync", "--json", "--keep", "1.5"],
    ["status", "--json", "--unknown"],
    ["status", "--json", "--json"],
    ["status", "--json=false"]
  ];
  for (const args of cases) {
    const result = await runNode(cliPath, args);
    assert.equal(result.code, 2, args.join(" "));
    assert.equal(result.stderr, "", args.join(" "));
    const envelope = parseSingleEnvelope(result.stdout);
    assert.equal(envelope.ok, false);
    assert.equal(envelope.error.code, "INVALID_INPUT");
    if (args[0] === "unknown") assert.equal(envelope.command, "unknown");
  }

  const human = await runNode(cliPath, ["unknown"]);
  assert.equal(human.code, 1);
  assert.equal(human.stdout, "");
  assert.match(human.stderr, /^Unknown command: unknown\r?\n$/);
});

test("JSON input and storage errors never echo untrusted values", async () => {
  const secretCommand = "fixtureSecretCommandValue";
  const unknownCommand = await runNode(cliPath, [secretCommand, "--json"]);
  assert.equal(unknownCommand.code, 2);
  assert.equal(parseSingleEnvelope(unknownCommand.stdout).command, "unknown");
  assert.doesNotMatch(`${unknownCommand.stdout}${unknownCommand.stderr}`, new RegExp(secretCommand));

  const secretProto = "fixtureSecretProto";
  const prototypeFlag = await runNode(cliPath, ["status", "--json", `--__proto__=${secretProto}`]);
  assert.equal(prototypeFlag.code, 2);
  assert.equal(parseSingleEnvelope(prototypeFlag.stdout).error.code, "INVALID_INPUT");
  assert.doesNotMatch(`${prototypeFlag.stdout}${prototypeFlag.stderr}`, new RegExp(secretProto));

  const secretKeep = "fixtureSecretToken123";
  const invalidKeep = await runNode(cliPath, ["sync", "--json", "--keep", secretKeep]);
  assert.equal(invalidKeep.code, 2);
  assert.equal(parseSingleEnvelope(invalidKeep.stdout).error.message, "The command input is invalid.");
  assert.doesNotMatch(`${invalidKeep.stdout}${invalidKeep.stderr}`, new RegExp(secretKeep));

  const missingHome = path.join(os.tmpdir(), "fixture-secret-home-value", "missing");
  const missing = await runNode(cliPath, ["status", "--json", "--codex-home", missingHome]);
  assert.equal(missing.code, 1);
  const envelope = parseSingleEnvelope(missing.stdout);
  assert.equal(envelope.error.code, "INTERNAL_ERROR");
  assert.equal(envelope.error.message, "An internal error occurred.");
  assert.doesNotMatch(`${missing.stdout}${missing.stderr}`, /fixture-secret-home-value/);
});

test("real CLI status JSON returns Core status without writing Human text", async () => {
  const parent = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-cli-status-"));
  const root = path.join(parent, "codex=tail");
  try {
    await fs.mkdir(path.join(root, "sessions"), { recursive: true });
    await fs.writeFile(path.join(root, "config.toml"), 'model_provider = "openai"\n', "utf8");
    const result = await runNode(cliPath, ["status", "--json", `--codex-home=${root}`]);
    assert.equal(result.code, 0);
    assert.equal(result.stderr, "");
    const envelope = parseSingleEnvelope(result.stdout);
    assert.equal(envelope.command, "status");
    assert.equal(envelope.ok, true);
    assert.equal(envelope.result.codexHome, path.resolve(root));
    assert.equal(envelope.result.currentProvider, "openai");
    assert.equal(envelope.error, null);
    assert.doesNotMatch(result.stdout, /^Codex home:/);
  } finally {
    await fs.rm(parent, { recursive: true, force: true });
  }
});

test("every finite JSON command returns the same top-level envelope", async () => {
  const cases = [
    ["status", ["status", "--json"]],
    ["sync", ["sync", "--json"]],
    ["switch", ["switch", "openai", "--model", "fixture-model", "--json"]],
    ["prune-backups", ["prune-backups", "--keep", "0", "--json"]],
    ["restore", ["restore", "C:\\fixture\\backup", "--no-config", "--json"]],
    ["install-windows-launcher", ["install-windows-launcher", "--json"]]
  ];

  for (const [command, args] of cases) {
    const result = await runNode(driverPath, args);
    assert.equal(result.code, 0, command);
    const envelope = parseSingleEnvelope(result.stdout);
    assert.equal(envelope.command, command);
    assert.equal(envelope.ok, true);
    assert.equal(envelope.outcome, "completed");
    assert.equal(envelope.error, null);
  }
});

test("JSON sync progress goes only to stderr and redacts the backup path", async () => {
  const result = await runNode(driverPath, ["sync", "--json"], { scenario: "warning" });
  assert.equal(result.code, 0);
  const envelope = parseSingleEnvelope(result.stdout);
  assert.equal(envelope.command, "sync");
  assert.deepEqual(envelope.warnings, ["Automatic backup cleanup did not complete."]);
  assert.match(result.stderr, /\[1\/6\] Scanning rollout files/);
  assert.match(result.stderr, /Backup created in 25 ms/);
  assert.doesNotMatch(result.stderr, /secret-backup-path/);
  assert.doesNotMatch(result.stdout, /\[1\/6\]|secret-backup-path/);
});

test("JSON subprocess exit matrix covers partial and canonical failures", async () => {
  const cases = [
    ["partial", 3, true, "partial", null],
    ["error:SYNC_FAILED_ROLLED_BACK", 1, false, "failed_rolled_back", "SYNC_FAILED_ROLLED_BACK"],
    ["error:INVALID_INPUT", 2, false, "failed", "INVALID_INPUT"],
    ["error:PLAN_EXPIRED", 2, false, "stale", "PLAN_EXPIRED"],
    ["error:STALE_STATE", 2, false, "stale", "STALE_STATE"],
    ["error:RECOVERY_REQUIRED", 4, false, "recovery_required", "RECOVERY_REQUIRED"],
    ["error:PENDING_TRANSACTION", 4, false, "recovery_required", "PENDING_TRANSACTION"],
    ["error:OPERATION_BUSY", 5, false, "failed", "OPERATION_BUSY"],
    ["error:LOCK_UNVERIFIABLE", 5, false, "failed", "LOCK_UNVERIFIABLE"],
    ["error:SQLITE_BUSY", 5, false, "failed", "SQLITE_BUSY"],
    ["error:OPERATION_CANCELLED", 130, false, "cancelled", "OPERATION_CANCELLED"]
  ];

  for (const [scenario, code, ok, outcome, errorCode] of cases) {
    const result = await runNode(driverPath, ["sync", "--json"], { scenario });
    assert.equal(result.code, code, scenario);
    const envelope = parseSingleEnvelope(result.stdout);
    assert.equal(envelope.ok, ok, scenario);
    assert.equal(envelope.outcome, outcome, scenario);
    assert.equal(envelope.error?.code ?? null, errorCode, scenario);
  }
});

test("serialization failure still emits one redacted INTERNAL_ERROR envelope", async () => {
  const result = await runNode(driverPath, ["sync", "--json"], { scenario: "cyclic-result" });
  assert.equal(result.code, 1);
  const envelope = parseSingleEnvelope(result.stdout);
  assert.equal(envelope.ok, false);
  assert.equal(envelope.error.code, "INTERNAL_ERROR");
  assert.equal(envelope.error.message, "An internal error occurred.");
  assert.doesNotMatch(result.stdout, /circular|stack|cause/i);
});

test("typed Core errors cannot inject unapproved details into JSON", async () => {
  const result = await runNode(driverPath, ["sync", "--json"], {
    scenario: "error-secret-details"
  });
  assert.equal(result.code, 5);
  const envelope = parseSingleEnvelope(result.stdout);
  assert.equal(envelope.error.code, "OPERATION_BUSY");
  assert.deepEqual(envelope.error.details, { busyScope: "codex-home" });
  assert.doesNotMatch(`${result.stdout}${result.stderr}`, /fixture-secret-token|fixture secret body/);
});

test("watch and web reject JSON before creating long-running state", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "codex-provider-sync-cli-json-"));
  try {
    for (const command of ["watch", "web"]) {
      const result = await runNode(cliPath, [command, "--json", "--codex-home", root]);
      assert.equal(result.code, 2, command);
      assert.equal(result.stderr, "", command);
      const envelope = parseSingleEnvelope(result.stdout);
      assert.equal(envelope.error.code, "INVALID_INPUT");
      assert.equal(envelope.error.message, "The command input is invalid.");
    }
    assert.deepEqual(await fs.readdir(root), []);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("watch and web JSON validation does not load Core or start Web", async () => {
  for (const args of [
    ["watch", "--once", "--json"],
    ["web", "--no-open", "--json"]
  ]) {
    let coreLoads = 0;
    let webStarts = 0;
    const stdoutChunks = [];
    const exitCode = await runCli(args, {
      stdout: {
        write(document) {
          stdoutChunks.push(document);
        }
      },
      stderr: { write() {} },
      loadCoreImpl: async () => {
        coreLoads += 1;
        return {};
      },
      startWebUiImpl: async () => {
        webStarts += 1;
      }
    });

    assert.equal(exitCode, 2, args[0]);
    assert.equal(coreLoads, 0, args[0]);
    assert.equal(webStarts, 0, args[0]);
    assert.equal(parseSingleEnvelope(stdoutChunks.join("")).error.code, "INVALID_INPUT");
  }
});

test("JSON terminal writer attempts stdout at most once when the pipe closes", async () => {
  let stdoutWrites = 0;
  let stderrWrites = 0;

  const exitCode = await runCli(["--json"], {
    stdout: {
      write() {
        stdoutWrites += 1;
        const error = new Error("pipe closed");
        error.code = "EPIPE";
        throw error;
      }
    },
    stderr: {
      write() {
        stderrWrites += 1;
      }
    }
  });

  assert.equal(exitCode, 1);
  assert.equal(stdoutWrites, 1);
  assert.equal(stderrWrites, 0);
});

test("JSON terminal writer handles asynchronous EPIPE without a second write", async () => {
  const stdout = new EventEmitter();
  let stdoutWrites = 0;
  let stderrWrites = 0;
  stdout.write = (_document, callback) => {
    stdoutWrites += 1;
    process.nextTick(() => {
      const error = new Error("pipe closed asynchronously");
      error.code = "EPIPE";
      stdout.emit("error", error);
      callback(error);
    });
    return true;
  };

  const exitCode = await runCli(["--json"], {
    stdout,
    stderr: {
      write() {
        stderrWrites += 1;
      }
    }
  });

  assert.equal(exitCode, 1);
  assert.equal(stdoutWrites, 1);
  assert.equal(stderrWrites, 0);
});

test("JSON progress stream failures cannot change the operation result", async () => {
  const stdoutChunks = [];
  const stderr = new EventEmitter();
  let stderrWrites = 0;
  stderr.write = (_document, callback) => {
    stderrWrites += 1;
    const error = new Error("diagnostic pipe closed");
    error.code = "EPIPE";
    stderr.emit("error", error);
    callback?.(error);
    return false;
  };

  const exitCode = await runCli(["sync", "--json"], {
    stdout: {
      write(document) {
        stdoutChunks.push(document);
      }
    },
    stderr,
    loadCoreImpl: async () => ({
      readConfigText: async () => "",
      readRootModelFromConfigText: () => null,
      runSync: async ({ onProgress }) => {
        onProgress({ stage: "scan_rollout_files", status: "start" });
        return { targetProvider: "openai", skippedLockedRolloutFiles: [] };
      }
    })
  });

  assert.equal(exitCode, 0);
  assert.equal(stderrWrites, 1);
  const envelope = parseSingleEnvelope(stdoutChunks.join(""));
  assert.equal(envelope.ok, true);
  assert.equal(envelope.outcome, "completed");
});
