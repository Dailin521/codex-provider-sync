export function applyPreparedDesktopOperationForTest(
  method: "applySync" | "applySwitch" | "applyRepair" | "applyRestore",
  input: { schemaVersion: 1; planId: string },
  control: {
    signal?: AbortSignal;
    onOperationStarted?(value: {
      operationId: string;
      operation: "sync" | "switch" | "repair" | "restore";
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
