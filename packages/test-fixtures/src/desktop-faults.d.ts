export function applyPreparedDesktopOperationForTest(
  method: "applySync" | "applySwitch" | "applyRestore",
  input: { schemaVersion: 1; planId: string },
  control: {
    signal?: AbortSignal;
    onOperationStarted?(value: {
      operationId: string;
      operation: "sync" | "switch" | "restore";
    }): void;
    onProgress?(event: {
      stage: string;
      status: string;
      progress?: number;
      count?: number;
    }): void;
  },
  faultInjector: (event: Record<string, unknown>) => void | Promise<void>
): Promise<unknown>;
