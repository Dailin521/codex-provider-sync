// @ts-nocheck

import { CoreError } from "../infrastructure/node-core-ports.js";

export const REPAIR_TARGET_ORDER = ["models", "cwd", "userEvent", "workspaceRoots"];
const SESSION_ID = /^[A-Za-z0-9_-]{1,128}$/;

export function normalizeRepairSessionIds(sessionIds, targets) {
  if (sessionIds === undefined) return null;
  if (!Array.isArray(sessionIds) || sessionIds.length === 0 || sessionIds.length > 100) {
    throw new CoreError("INVALID_INPUT", "Repair sessionIds must contain 1 to 100 native session IDs.");
  }
  if (targets.includes("workspaceRoots")) {
    throw new CoreError("INVALID_INPUT", "workspaceRoots repair is whole-profile only.");
  }
  const result = new Set();
  for (const id of sessionIds) {
    if (typeof id !== "string" || !SESSION_ID.test(id) || result.has(id)) {
      throw new CoreError("INVALID_INPUT", "Repair sessionIds must be unique native session IDs.");
    }
    result.add(id);
  }
  return result;
}

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
