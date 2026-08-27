import type {
  DesktopUpdateReason,
  DesktopUpdateStatus
} from "../shared/update-types.js";
import {
  getDesktopInstallBlockedReason,
  getDesktopUpdateUnavailableReason
} from "./update-policy.js";
import type { CoreRuntimeSupervisor } from "./runtime-supervisor.js";

type UpdaterEvent =
  | "checking-for-update"
  | "update-available"
  | "update-not-available"
  | "download-progress"
  | "update-downloaded"
  | "error";

type UpdaterListener = (value?: unknown) => void;

export interface DesktopUpdaterPort {
  on(event: UpdaterEvent, listener: UpdaterListener): void;
  off(event: UpdaterEvent, listener: UpdaterListener): void;
  checkForUpdates(): Promise<unknown>;
  downloadUpdate(): Promise<unknown>;
  quitAndInstall(isSilent?: boolean, isForceRunAfter?: boolean): void;
}

export type DesktopRecoveryVerification = "clear" | "blocked" | "unverifiable";

export interface DesktopUpdateControllerOptions {
  isPackaged: boolean;
  platform: NodeJS.Platform;
  arch: string;
  appVersion: string;
  configured: boolean;
  supervisor: Pick<CoreRuntimeSupervisor, "snapshot" | "tryBeginRestartInstall">;
  hasActiveWatches(): boolean;
  verifyRecoveryState(): Promise<DesktopRecoveryVerification>;
  beforeInstall?(): Promise<void>;
  createPort?: () => Promise<DesktopUpdaterPort>;
  setTimeoutImpl?: typeof setTimeout;
  clearTimeoutImpl?: typeof clearTimeout;
}

function safeVersion(value: unknown): string | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const version = (value as { version?: unknown }).version;
  if (typeof version !== "string" || !/^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$/.test(version)) {
    return undefined;
  }
  return version;
}

function safeProgressPercent(value: unknown): number | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const percent = (value as { percent?: unknown }).percent;
  if (typeof percent !== "number" || !Number.isFinite(percent)) return undefined;
  return Math.max(0, Math.min(100, Math.round(percent)));
}

export async function createProductionUpdaterPort(options: {
  allowPrerelease: boolean;
}): Promise<DesktopUpdaterPort> {
  const { autoUpdater } = await import("electron-updater");
  autoUpdater.autoDownload = false;
  autoUpdater.autoInstallOnAppQuit = false;
  autoUpdater.allowPrerelease = options.allowPrerelease;
  autoUpdater.logger = null;
  return {
    on(event, listener) {
      autoUpdater.on(event, listener);
    },
    off(event, listener) {
      autoUpdater.off(event, listener);
    },
    checkForUpdates() {
      return autoUpdater.checkForUpdates();
    },
    downloadUpdate() {
      return autoUpdater.downloadUpdate();
    },
    quitAndInstall(isSilent, isForceRunAfter) {
      autoUpdater.quitAndInstall(isSilent, isForceRunAfter);
    }
  };
}

export class DesktopUpdateController {
  readonly #supervisor: Pick<CoreRuntimeSupervisor, "snapshot" | "tryBeginRestartInstall">;
  readonly #hasActiveWatches: () => boolean;
  readonly #verifyRecoveryState: () => Promise<DesktopRecoveryVerification>;
  readonly #beforeInstall: () => Promise<void>;
  readonly #createPort: () => Promise<DesktopUpdaterPort>;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #unavailableReason: ReturnType<typeof getDesktopUpdateUnavailableReason>;
  readonly #listeners = new Map<UpdaterEvent, UpdaterListener>();
  #state: DesktopUpdateStatus["state"];
  #reason: DesktopUpdateReason | undefined;
  #version: string | undefined;
  #progressPercent: number | undefined;
  #recoveryVerification: "unknown" | "clear" | "blocked" = "unknown";
  #restartPending = false;
  #port: DesktopUpdaterPort | null = null;
  #portPromise: Promise<DesktopUpdaterPort> | null = null;
  #checkPromise: Promise<DesktopUpdateStatus> | null = null;
  #downloadPromise: Promise<DesktopUpdateStatus> | null = null;
  #installPromise: Promise<DesktopUpdateStatus> | null = null;
  #initialCheckTimer: ReturnType<typeof setTimeout> | null = null;
  #disposed = false;

  constructor(options: DesktopUpdateControllerOptions) {
    this.#supervisor = options.supervisor;
    this.#hasActiveWatches = options.hasActiveWatches;
    this.#verifyRecoveryState = options.verifyRecoveryState;
    this.#beforeInstall = options.beforeInstall ?? (async () => {});
    this.#createPort = options.createPort ?? (() => createProductionUpdaterPort({
      allowPrerelease: options.appVersion.includes("-")
    }));
    this.#setTimeout = options.setTimeoutImpl ?? setTimeout;
    this.#clearTimeout = options.clearTimeoutImpl ?? clearTimeout;
    this.#unavailableReason = getDesktopUpdateUnavailableReason(options);
    this.#state = this.#unavailableReason ? "disabled" : "idle";
    this.#reason = this.#unavailableReason ?? undefined;
  }

  get restartPending(): boolean {
    return this.#restartPending;
  }

  get status(): DesktopUpdateStatus {
    const status: DesktopUpdateStatus = {
      schemaVersion: 2,
      state: this.#state,
      installAllowed: false,
      ...(this.#reason ? { reason: this.#reason } : {}),
      ...(this.#version ? { version: this.#version } : {}),
      ...(this.#progressPercent !== undefined ? { progressPercent: this.#progressPercent } : {})
    };
    if (this.#state !== "downloaded") return status;
    const blocked = this.#recoveryVerification === "blocked"
      ? "pending-recovery"
      : getDesktopInstallBlockedReason({
        supervisor: this.#supervisor,
        hasActiveWatches: this.#hasActiveWatches(),
        recoveryVerified: this.#recoveryVerification === "clear"
      });
    return {
      ...status,
      installAllowed: blocked === null,
      ...(blocked ? { installBlockedReason: blocked } : {})
    };
  }

  scheduleInitialCheck(delayMs = 15_000): void {
    if (this.#disposed || this.#unavailableReason || this.#initialCheckTimer) return;
    const timer = this.#setTimeout(() => {
      if (this.#initialCheckTimer !== timer) return;
      this.#initialCheckTimer = null;
      void this.check();
    }, Math.max(0, delayMs));
    timer.unref?.();
    this.#initialCheckTimer = timer;
  }

  async check(): Promise<DesktopUpdateStatus> {
    if (this.#disposed || this.#unavailableReason || this.#restartPending) return this.status;
    if (this.#checkPromise) return this.#checkPromise;
    if (this.#state === "downloading" || this.#state === "downloaded" || this.#state === "installing") {
      return this.status;
    }
    const pending = (async () => {
      this.#state = "checking";
      this.#reason = undefined;
      this.#version = undefined;
      this.#progressPercent = undefined;
      this.#recoveryVerification = "unknown";
      try {
        const port = await this.#ensurePort();
        const result = await port.checkForUpdates();
        if (this.#state === "checking") {
          const resultVersion = safeVersion(
            result && typeof result === "object" && !Array.isArray(result)
              ? (result as { updateInfo?: unknown }).updateInfo
              : undefined
          );
          this.#version = resultVersion;
          this.#state = resultVersion ? "available" : "not-available";
        }
      } catch {
        this.#fail("check-failed");
      }
      return this.status;
    })();
    this.#checkPromise = pending;
    try {
      return await pending;
    } finally {
      if (this.#checkPromise === pending) this.#checkPromise = null;
    }
  }

  async download(): Promise<DesktopUpdateStatus> {
    if (this.#disposed || this.#restartPending || this.#state !== "available") return this.status;
    if (this.#downloadPromise) return this.#downloadPromise;
    const pending = (async () => {
      this.#state = "downloading";
      this.#reason = undefined;
      this.#progressPercent = 0;
      try {
        const port = await this.#ensurePort();
        await port.downloadUpdate();
        if (this.#state === "downloading") {
          this.#state = "downloaded";
          this.#progressPercent = 100;
        }
        if (this.#state === "downloaded") await this.#refreshRecoveryVerification();
      } catch {
        this.#fail("download-failed");
      }
      return this.status;
    })();
    this.#downloadPromise = pending;
    try {
      return await pending;
    } finally {
      if (this.#downloadPromise === pending) this.#downloadPromise = null;
    }
  }

  async install(): Promise<DesktopUpdateStatus> {
    if (this.#disposed || this.#state !== "downloaded") return this.status;
    if (this.#installPromise) return this.#installPromise;
    const pending = (async () => {
      const restartLease = this.#supervisor.tryBeginRestartInstall();
      if (!restartLease) {
        this.#recoveryVerification = "unknown";
        return this.status;
      }
      this.#restartPending = true;
      let retainRestartGate = false;
      try {
        await restartLease.waitForWrites();
        this.#recoveryVerification = "unknown";
        const recoveryVerification = await this.#refreshRecoveryVerification();
        if (this.#immediateInstallBlock() || recoveryVerification !== "clear") {
          return this.status;
        }
        this.#state = "installing";
        const port = await this.#ensurePort();
        await this.#beforeInstall();
        if (this.#immediateInstallBlock()) {
          this.#state = "downloaded";
          this.#recoveryVerification = "unknown";
          return this.status;
        }
        port.quitAndInstall(false, true);
        retainRestartGate = true;
      } catch {
        this.#fail("install-failed");
      } finally {
        if (!retainRestartGate) {
          restartLease.release();
          this.#restartPending = false;
        }
      }
      return this.status;
    })();
    this.#installPromise = pending;
    try {
      return await pending;
    } finally {
      if (this.#installPromise === pending) this.#installPromise = null;
    }
  }

  dispose(): void {
    this.#disposed = true;
    if (this.#initialCheckTimer) {
      this.#clearTimeout(this.#initialCheckTimer);
      this.#initialCheckTimer = null;
    }
    if (this.#port) {
      for (const [event, listener] of this.#listeners) this.#port.off(event, listener);
    }
    this.#listeners.clear();
  }

  #immediateInstallBlock(): boolean {
    return this.#supervisor.snapshot.recoveryBlocked
      || this.#supervisor.snapshot.writeInProgress
      || this.#hasActiveWatches();
  }

  async #refreshRecoveryVerification(): Promise<"unknown" | "clear" | "blocked"> {
    if (this.#immediateInstallBlock()) {
      this.#recoveryVerification = this.#supervisor.snapshot.recoveryBlocked
        ? "blocked"
        : "unknown";
      return this.#recoveryVerification;
    }
    try {
      const result = await this.#verifyRecoveryState();
      this.#recoveryVerification = result === "clear"
        ? "clear"
        : result === "blocked"
          ? "blocked"
          : "unknown";
    } catch {
      this.#recoveryVerification = "unknown";
    }
    return this.#recoveryVerification;
  }

  async #ensurePort(): Promise<DesktopUpdaterPort> {
    if (this.#port) return this.#port;
    if (!this.#portPromise) {
      this.#portPromise = this.#createPort().then((port) => {
        if (this.#disposed) throw new Error("Update controller is disposed.");
        this.#port = port;
        this.#bindPort(port);
        return port;
      });
    }
    try {
      return await this.#portPromise;
    } finally {
      if (!this.#port) this.#portPromise = null;
    }
  }

  #bindPort(port: DesktopUpdaterPort): void {
    const bind = (event: UpdaterEvent, listener: UpdaterListener) => {
      this.#listeners.set(event, listener);
      port.on(event, listener);
    };
    bind("checking-for-update", () => {
      if (!this.#restartPending) this.#state = "checking";
    });
    bind("update-available", (info) => {
      if (this.#restartPending) return;
      const version = safeVersion(info);
      if (!version) {
        this.#fail("check-failed");
        return;
      }
      this.#state = "available";
      this.#reason = undefined;
      this.#version = version;
      this.#progressPercent = undefined;
      this.#recoveryVerification = "unknown";
    });
    bind("update-not-available", () => {
      if (this.#restartPending) return;
      this.#state = "not-available";
      this.#reason = undefined;
      this.#version = undefined;
      this.#progressPercent = undefined;
      this.#recoveryVerification = "unknown";
    });
    bind("download-progress", (progress) => {
      if (this.#state !== "downloading") return;
      const percent = safeProgressPercent(progress);
      if (percent !== undefined) this.#progressPercent = percent;
    });
    bind("update-downloaded", (info) => {
      if (this.#restartPending) return;
      const version = safeVersion(info) ?? this.#version;
      if (!version) {
        this.#fail("download-failed");
        return;
      }
      this.#state = "downloaded";
      this.#reason = undefined;
      this.#version = version;
      this.#progressPercent = 100;
      void this.#refreshRecoveryVerification();
    });
    bind("error", () => {
      if (this.#restartPending) return;
      this.#fail(this.#state === "downloading" ? "download-failed" : "check-failed");
    });
  }

  #fail(reason: DesktopUpdateReason): void {
    this.#state = "error";
    this.#reason = reason;
    this.#progressPercent = undefined;
    this.#recoveryVerification = "unknown";
  }
}
