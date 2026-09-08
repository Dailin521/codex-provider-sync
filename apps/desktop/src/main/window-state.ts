import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

export interface WindowBounds {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface WindowDisplay {
  id: string | number;
  workArea: WindowBounds;
}

export interface WindowStateScreen {
  getAllDisplays(): WindowDisplay[];
  getPrimaryDisplay(): WindowDisplay;
  getDisplayMatching(bounds: WindowBounds): WindowDisplay;
}

export interface WindowStateWindow {
  getNormalBounds(): WindowBounds;
  isMaximized(): boolean;
  on(event: "move" | "resize" | "maximize" | "unmaximize", listener: () => void): unknown;
  removeListener(event: "move" | "resize" | "maximize" | "unmaximize", listener: () => void): unknown;
}

export interface PersistedWindowState {
  schemaVersion: 1;
  normalBounds: WindowBounds;
  maximized: boolean;
  displayId: string;
}

export interface RestoredWindowState {
  bounds: WindowBounds;
  maximized: boolean;
}

const SCHEMA_VERSION = 1 as const;
const MAX_STATE_BYTES = 2048;
const MIN_PERSISTED_SIZE = 100;
const MAX_PERSISTED_SIZE = 100_000;
const MAX_COORDINATE = 1_000_000;

function isFiniteInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

function isPersistedBounds(value: unknown): value is WindowBounds {
  if (!value || typeof value !== "object") return false;
  const bounds = value as Record<string, unknown>;
  return isFiniteInteger(bounds.x)
    && isFiniteInteger(bounds.y)
    && isFiniteInteger(bounds.width)
    && isFiniteInteger(bounds.height)
    && Math.abs(bounds.x) <= MAX_COORDINATE
    && Math.abs(bounds.y) <= MAX_COORDINATE
    && bounds.width >= MIN_PERSISTED_SIZE
    && bounds.height >= MIN_PERSISTED_SIZE
    && bounds.width <= MAX_PERSISTED_SIZE
    && bounds.height <= MAX_PERSISTED_SIZE;
}

export function parseWindowState(source: string): PersistedWindowState | null {
  if (Buffer.byteLength(source, "utf8") > MAX_STATE_BYTES) return null;
  try {
    const value = JSON.parse(source) as Record<string, unknown>;
    if (value?.schemaVersion !== SCHEMA_VERSION
      || !isPersistedBounds(value.normalBounds)
      || typeof value.maximized !== "boolean"
      || typeof value.displayId !== "string"
      || value.displayId.length === 0
      || value.displayId.length > 128) return null;
    return {
      schemaVersion: SCHEMA_VERSION,
      normalBounds: { ...value.normalBounds },
      maximized: value.maximized,
      displayId: value.displayId
    };
  } catch {
    return null;
  }
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(Math.max(value, minimum), Math.max(minimum, maximum));
}

function usableWorkArea(display: WindowDisplay | undefined): WindowBounds | null {
  const workArea = display?.workArea;
  if (!workArea || !isFiniteInteger(workArea.x) || !isFiniteInteger(workArea.y)
    || !isFiniteInteger(workArea.width) || !isFiniteInteger(workArea.height)
    || workArea.width <= 0 || workArea.height <= 0) return null;
  return workArea;
}

function displayFor(state: PersistedWindowState, screen: WindowStateScreen): WindowDisplay {
  const displays = screen.getAllDisplays();
  return displays.find((display) => String(display.id) === state.displayId)
    ?? screen.getDisplayMatching(state.normalBounds)
    ?? screen.getPrimaryDisplay();
}

export function restoreWindowState(
  state: PersistedWindowState | null,
  screen: WindowStateScreen,
  fallback: WindowBounds,
  minimumSize: Pick<WindowBounds, "width" | "height">
): RestoredWindowState {
  if (!state) return { bounds: { ...fallback }, maximized: false };
  const workArea = usableWorkArea(displayFor(state, screen));
  if (!workArea) return { bounds: { ...fallback }, maximized: false };
  const minimumWidth = Math.min(Math.max(1, minimumSize.width), workArea.width);
  const minimumHeight = Math.min(Math.max(1, minimumSize.height), workArea.height);
  const width = clamp(state.normalBounds.width, minimumWidth, workArea.width);
  const height = clamp(state.normalBounds.height, minimumHeight, workArea.height);
  return {
    bounds: {
      width,
      height,
      x: clamp(state.normalBounds.x, workArea.x, workArea.x + workArea.width - width),
      y: clamp(state.normalBounds.y, workArea.y, workArea.y + workArea.height - height)
    },
    maximized: state.maximized
  };
}

/** Refit a live window after display removal or scaling/work-area changes. */
export function fitWindowToDisplays(window: WindowStateWindow & {
  setMinimumSize(width: number, height: number): void;
  setBounds(bounds: WindowBounds): void;
  maximize(): void;
  unmaximize(): void;
}, screen: WindowStateScreen): void {
  const bounds = window.getNormalBounds();
  const display = screen.getDisplayMatching(bounds) ?? screen.getPrimaryDisplay();
  const workArea = usableWorkArea(display);
  if (!workArea || !isPersistedBounds(bounds)) return;
  const maximized = window.isMaximized();
  const fitted = restoreWindowState({ schemaVersion: 1, normalBounds: bounds, maximized, displayId: String(display.id) }, screen, bounds, { width: 760, height: 560 });
  window.setMinimumSize(Math.min(760, workArea.width), Math.min(560, workArea.height));
  if (Object.keys(bounds).every((key) => bounds[key as keyof WindowBounds] === fitted.bounds[key as keyof WindowBounds])) return;
  if (maximized) window.unmaximize();
  window.setBounds(fitted.bounds);
  if (maximized) window.maximize();
}

export class WindowStateStore {
  readonly #filePath: string;
  readonly #enabled: boolean;

  constructor(options: { filePath: string; enabled?: boolean }) {
    this.#filePath = options.filePath;
    this.#enabled = options.enabled ?? true;
  }

  async read(): Promise<PersistedWindowState | null> {
    if (!this.#enabled) return null;
    try {
      const metadata = await fs.stat(this.#filePath);
      if (metadata.size <= 0 || metadata.size > MAX_STATE_BYTES) return null;
      return parseWindowState(await fs.readFile(this.#filePath, "utf8"));
    } catch {
      return null;
    }
  }

  async write(state: PersistedWindowState): Promise<void> {
    if (!this.#enabled) return;
    const serialized = `${JSON.stringify(state)}\n`;
    if (Buffer.byteLength(serialized, "utf8") > MAX_STATE_BYTES) return;
    const directory = path.dirname(this.#filePath);
    const temporary = path.join(directory, `.${path.basename(this.#filePath)}.${process.pid}.${randomUUID()}.tmp`);
    try {
      await fs.mkdir(directory, { recursive: true });
      const handle = await fs.open(temporary, "w", 0o600);
      try {
        await handle.writeFile(serialized, "utf8");
        await handle.sync();
      } finally {
        await handle.close();
      }
      await fs.rename(temporary, this.#filePath);
    } catch {
      await fs.rm(temporary, { force: true }).catch(() => {});
    }
  }
}

export class WindowStateController {
  readonly #store: WindowStateStore;
  readonly #screen: WindowStateScreen;
  readonly #debounceMs: number;
  #state: PersistedWindowState | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #write: Promise<void> = Promise.resolve();

  constructor(options: { store: WindowStateStore; screen: WindowStateScreen; debounceMs?: number }) {
    this.#store = options.store;
    this.#screen = options.screen;
    this.#debounceMs = options.debounceMs ?? 350;
  }

  attach(window: WindowStateWindow): () => void {
    const onChange = () => {
      this.capture(window);
      this.#schedule();
    };
    for (const event of ["move", "resize", "maximize", "unmaximize"] as const) window.on(event, onChange);
    return () => {
      for (const event of ["move", "resize", "maximize", "unmaximize"] as const) window.removeListener(event, onChange);
    };
  }

  capture(window: WindowStateWindow): void {
    const normalBounds = window.getNormalBounds();
    if (!isPersistedBounds(normalBounds)) return;
    const display = this.#screen.getDisplayMatching(normalBounds) ?? this.#screen.getPrimaryDisplay();
    this.#state = {
      schemaVersion: SCHEMA_VERSION,
      normalBounds: { ...normalBounds },
      maximized: window.isMaximized(),
      displayId: String(display.id)
    };
  }

  async flush(window?: WindowStateWindow): Promise<void> {
    if (window) this.capture(window);
    if (this.#timer) {
      clearTimeout(this.#timer);
      this.#timer = null;
    }
    this.#persist();
    await this.#write;
  }

  #schedule(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#persist();
    }, this.#debounceMs);
  }

  #persist(): void {
    const state = this.#state;
    if (!state) return;
    const next = this.#write.then(() => this.#store.write(state));
    this.#write = next.catch(() => {});
  }
}
