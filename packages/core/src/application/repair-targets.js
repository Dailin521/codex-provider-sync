// @ts-nocheck

import { CoreError } from "../infrastructure/node-core-ports.js";

export const REPAIR_TARGET_ORDER = ["models", "cwd", "userEvent", "workspaceRoots"];

export function normalizeRepairTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) {
    throw new CoreError("INVALID_INPUT", "Repair requires at least one target.");
  }
  const selected = new Set();
  for (const target of targets) {
    if (typeof target !== "string" || !REPAIR_TARGET_ORDER.includes(target)) {
      throw new CoreError("INVALID_INPUT", `Unknown Repair target: ${String(target)}.`);
    }
    if (selected.has(target)) throw new CoreError("INVALID_INPUT", `Duplicate Repair target: ${target}.`);
    selected.add(target);
  }
  if (selected.has("workspaceRoots")) selected.add("cwd");
  return REPAIR_TARGET_ORDER.filter((target) => selected.has(target));
}

export function repairSqliteRowsToChange(stats, targets) {
  const selected = new Set(targets);
  return (selected.has("models") ? stats?.modelRowsNeedingRepair ?? 0 : 0)
    + (selected.has("cwd") ? stats?.cwdRowsNeedingRepair ?? 0 : 0)
    + (selected.has("userEvent") ? stats?.userEventRowsNeedingRepair ?? 0 : 0);
}
