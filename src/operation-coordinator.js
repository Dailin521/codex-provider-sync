import { randomUUID } from "node:crypto";
import path from "node:path";

import { CoreError } from "./core-error.js";

function homeKey(codexHome, platform = process.platform) {
  const resolved = path.resolve(codexHome);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}

function clone(value) {
  return value === null || value === undefined
    ? value
    : JSON.parse(JSON.stringify(value));
}

export class OperationCoordinator {
  constructor({ randomOperationId = randomUUID, now = () => Date.now() } = {}) {
    this.randomOperationId = randomOperationId;
    this.now = now;
    this.active = new Map();
    this.snapshots = new Map();
    this.endListeners = new Map();
  }

  cacheStatus(codexHome, snapshot, platform = process.platform) {
    this.snapshots.set(homeKey(codexHome, platform), clone(snapshot));
  }

  statusDuringWrite(codexHome, platform = process.platform, expectedProfile = null) {
    const key = homeKey(codexHome, platform);
    const operation = this.active.get(key);
    if (!operation) return null;
    return this.statusForBlockedWrite(codexHome, operation, platform, expectedProfile);
  }

  statusForBlockedWrite(codexHome, operation, platform = process.platform, expectedProfile = null) {
    const key = homeKey(codexHome, platform);
    const candidate = this.snapshots.get(key);
    const cached = candidate
      && (!expectedProfile
        || (candidate.profileId === expectedProfile.id
          && candidate.profileRevision === expectedProfile.publicRevision))
      ? candidate
      : null;
    if (!cached) {
      return {
        schemaVersion: 1,
        snapshotAt: new Date(this.now()).toISOString(),
        codexHome: path.resolve(codexHome),
        operationInProgress: clone(operation),
        rolloutScanComplete: false,
        lockedRolloutFiles: []
      };
    }
    return {
      ...clone(cached),
      operationInProgress: clone(operation)
    };
  }

  begin(codexHome, operation, { actor = "manual", platform = process.platform } = {}) {
    const key = homeKey(codexHome, platform);
    if (this.active.has(key)) {
      throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
        details: { busyScope: "codex-home" }
      });
    }
    const active = Object.freeze({
      operationId: this.randomOperationId(),
      operation,
      actor,
      startedAt: new Date(this.now()).toISOString()
    });
    this.active.set(key, active);
    return active;
  }

  end(codexHome, operationId, platform = process.platform) {
    const key = homeKey(codexHome, platform);
    if (this.active.get(key)?.operationId !== operationId) return;
    const completed = this.active.get(key);
    this.active.delete(key);
    const listeners = this.endListeners.get(key);
    if (!listeners) return;
    this.endListeners.delete(key);
    for (const listener of listeners) {
      queueMicrotask(() => listener(clone(completed)));
    }
  }

  waitForManualOperation(codexHome, platform = process.platform) {
    const key = homeKey(codexHome, platform);
    if (this.active.get(key)?.actor !== "manual") return null;
    let listener;
    const promise = new Promise((resolve) => {
      listener = resolve;
      const listeners = this.endListeners.get(key) ?? new Set();
      listeners.add(listener);
      this.endListeners.set(key, listeners);
    });
    return {
      promise,
      cancel: () => {
        const listeners = this.endListeners.get(key);
        listeners?.delete(listener);
        if (listeners?.size === 0) this.endListeners.delete(key);
      }
    };
  }

  hasManualOperation(codexHome, platform = process.platform) {
    return this.active.get(homeKey(codexHome, platform))?.actor === "manual";
  }

  isActive(codexHome, platform = process.platform) {
    return this.active.has(homeKey(codexHome, platform));
  }
}

export const sharedOperationCoordinator = new OperationCoordinator();
