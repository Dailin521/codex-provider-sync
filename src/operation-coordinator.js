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
  constructor({
    randomOperationId = randomUUID,
    now = () => Date.now(),
    setTimeoutImpl = setTimeout,
    clearTimeoutImpl = clearTimeout
  } = {}) {
    this.randomOperationId = randomOperationId;
    this.now = now;
    this.setTimeoutImpl = setTimeoutImpl;
    this.clearTimeoutImpl = clearTimeoutImpl;
    this.active = new Map();
    this.snapshots = new Map();
    this.manualIntents = new Map();
    this.manualIntentExpiryTimers = new Map();
    this.manualPriorityWaiters = new Map();
  }

  _sweepManualIntents(key) {
    const intents = this.manualIntents.get(key);
    if (!intents) return null;
    const now = this.now();
    for (const [planId, expiresAtMs] of intents) {
      if (now >= expiresAtMs) intents.delete(planId);
    }
    if (intents.size === 0) {
      this.manualIntents.delete(key);
      return null;
    }
    return intents;
  }

  _hasManualPriority(key) {
    return this.active.get(key)?.actor === "manual"
      || Boolean(this._sweepManualIntents(key)?.size);
  }

  _clearManualIntentExpiryTimer(key) {
    const timer = this.manualIntentExpiryTimers.get(key);
    if (timer !== undefined) this.clearTimeoutImpl(timer);
    this.manualIntentExpiryTimers.delete(key);
  }

  _armManualIntentExpiryTimer(key) {
    this._clearManualIntentExpiryTimer(key);
    const intents = this._sweepManualIntents(key);
    if (!intents?.size) return;
    const earliestExpiry = Math.min(...intents.values());
    const timer = this.setTimeoutImpl(() => {
      if (this.manualIntentExpiryTimers.get(key) !== timer) return;
      this.manualIntentExpiryTimers.delete(key);
      this._sweepManualIntents(key);
      this._armManualIntentExpiryTimer(key);
      this._settleManualPriorityWaiters(key);
    }, Math.max(0, earliestExpiry - this.now()));
    timer?.unref?.();
    this.manualIntentExpiryTimers.set(key, timer);
  }

  _settleManualPriorityWaiters(key) {
    const waiters = this.manualPriorityWaiters.get(key);
    if (!waiters) return;
    if (this._hasManualPriority(key)) {
      return;
    }
    this.manualPriorityWaiters.delete(key);
    for (const waiter of waiters) {
      waiter.resolve();
    }
  }

  registerManualIntent(codexHome, planId, expiresAt, platform = process.platform) {
    if (typeof planId !== "string" || !planId) {
      throw new TypeError("Manual intent planId must be a non-empty string.");
    }
    const expiresAtMs = typeof expiresAt === "number" ? expiresAt : Date.parse(expiresAt);
    if (!Number.isFinite(expiresAtMs)) {
      throw new TypeError("Manual intent expiresAt must be a valid timestamp.");
    }
    const key = homeKey(codexHome, platform);
    const intents = this._sweepManualIntents(key) ?? new Map();
    intents.set(planId, expiresAtMs);
    this.manualIntents.set(key, intents);
    this._armManualIntentExpiryTimer(key);
  }

  releaseManualIntent(codexHome, planId, platform = process.platform) {
    const key = homeKey(codexHome, platform);
    const intents = this.manualIntents.get(key);
    if (intents) {
      intents.delete(planId);
      if (intents.size === 0) this.manualIntents.delete(key);
    }
    this._armManualIntentExpiryTimer(key);
    this._settleManualPriorityWaiters(key);
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

  begin(codexHome, operation, {
    actor = "manual",
    planId = null,
    platform = process.platform
  } = {}) {
    const key = homeKey(codexHome, platform);
    if (this.active.has(key)) {
      if (actor === "manual" && planId) {
        this.releaseManualIntent(codexHome, planId, platform);
      }
      throw new CoreError("OPERATION_BUSY", "Lock already exists for this Codex Home; another write operation is active.", {
        details: { busyScope: "codex-home" }
      });
    }
    if (actor === "watch" && this._sweepManualIntents(key)?.size) {
      throw new CoreError("OPERATION_BUSY", "A confirmed manual operation has priority for this Codex Home.", {
        details: { busyScope: "codex-home", reason: "manual-intent" }
      });
    }
    try {
      const active = Object.freeze({
        operationId: this.randomOperationId(),
        operation,
        actor,
        startedAt: new Date(this.now()).toISOString()
      });
      this.active.set(key, active);
      if (actor === "manual" && planId) {
        this.releaseManualIntent(codexHome, planId, platform);
      }
      return active;
    } catch (error) {
      if (actor === "manual" && planId) {
        this.releaseManualIntent(codexHome, planId, platform);
      }
      throw error;
    }
  }

  end(codexHome, operationId, platform = process.platform) {
    const key = homeKey(codexHome, platform);
    if (this.active.get(key)?.operationId !== operationId) return;
    this.active.delete(key);
    this._settleManualPriorityWaiters(key);
  }

  waitForManualOperation(codexHome, platform = process.platform) {
    const key = homeKey(codexHome, platform);
    if (!this._hasManualPriority(key)) return null;
    let waiter;
    const promise = new Promise((resolve) => {
      waiter = { resolve };
      const waiters = this.manualPriorityWaiters.get(key) ?? new Set();
      waiters.add(waiter);
      this.manualPriorityWaiters.set(key, waiters);
    });
    return {
      promise,
      cancel: () => {
        const waiters = this.manualPriorityWaiters.get(key);
        waiters?.delete(waiter);
        if (waiters?.size === 0) this.manualPriorityWaiters.delete(key);
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
