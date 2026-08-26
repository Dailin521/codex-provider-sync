import fs from "node:fs/promises";
import path from "node:path";

import { applyPreparedDesktopOperationForTest } from "@codex-provider-sync/test-fixtures/desktop-faults";

import type { DesktopRuntimeTestApplyInvoker } from "./host.js";

const ALLOWED_TEST_GATE_POINTS = new Set([
  "before_backup",
  "after_config_mutation_before_applied",
  "after_rollout_mutation_before_applied",
  "after_sqlite_commit_before_ack",
  "after_transaction_journal_commit_before_ack",
  "after_transaction_commit"
]);

function cancelledAtTestGate(): Error & { code: "ABORT_ERR" } {
  const error = new Error("Desktop E2E operation cancelled at a deterministic safety gate.") as Error & {
    code: "ABORT_ERR";
  };
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  return error;
}

function createDesktopTestFaultInjector(signal: AbortSignal):
  (event: Record<string, unknown>) => Promise<void> {
  const selectedPoint = process.env.CPS_DESKTOP_TEST_GATE;
  const markerPath = process.env.CPS_DESKTOP_TEST_GATE_FILE;
  if (!selectedPoint
      || !markerPath
      || !ALLOWED_TEST_GATE_POINTS.has(selectedPoint)
      || !path.isAbsolute(markerPath)) {
    throw new Error("Invalid desktop E2E fault gate configuration.");
  }
  let entered = false;
  return async (event) => {
    if (entered || event.point !== selectedPoint) return;
    entered = true;
    await fs.mkdir(path.dirname(markerPath), { recursive: true });
    await fs.writeFile(markerPath, `${JSON.stringify({ point: selectedPoint })}\n`, {
      encoding: "utf8",
      flag: "wx"
    });
    if (!signal.aborted) {
      await new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
    }
    throw cancelledAtTestGate();
  };
}

export function createDesktopTestApplyInvoker(): DesktopRuntimeTestApplyInvoker | undefined {
  const selectedPoint = process.env.CPS_DESKTOP_TEST_GATE;
  const markerPath = process.env.CPS_DESKTOP_TEST_GATE_FILE;
  if (!selectedPoint && !markerPath) return undefined;
  if (!selectedPoint
      || !markerPath
      || !ALLOWED_TEST_GATE_POINTS.has(selectedPoint)
      || !path.isAbsolute(markerPath)) {
    throw new Error("Invalid desktop E2E fault gate configuration.");
  }
  return async (method, input, control) => {
    return applyPreparedDesktopOperationForTest(
      method,
      input,
      control,
      createDesktopTestFaultInjector(control.signal ?? new AbortController().signal)
    ) as ReturnType<DesktopRuntimeTestApplyInvoker>;
  };
}
