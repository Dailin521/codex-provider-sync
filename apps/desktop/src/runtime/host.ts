import {
  createCoreFailureEnvelope,
  createCoreSuccessEnvelope,
  createPublicCoreErrorDto,
  sanitizePublicCoreErrorDto,
  type CoreMethodMap,
  type CoreOperationStartedEnvelope,
  type CoreProgressEnvelope,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope,
  type ProgressEvent
} from "@codex-provider-sync/contracts";
import { createCoreFacade } from "@codex-provider-sync/core";
import type { DesktopRuntimeMethod } from "@codex-provider-sync/core-client";

import type { DesktopProfileRepository } from "../profiles/repository.js";

export interface DesktopRuntimeDispatchControl {
  signal?: AbortSignal;
  onOperationStarted?(event: CoreOperationStartedEnvelope): void;
  onProgress?(event: CoreProgressEnvelope): void;
}

export type DesktopRuntimeTestApplyInvoker = (
  method: "applySync" | "applySwitch" | "applyRestore",
  input: CoreMethodMap["applySync"]["input"],
  control: {
    signal?: AbortSignal;
    onOperationStarted?(value: {
      operationId: string;
      operation: "sync" | "switch" | "restore";
    }): void;
    onProgress?(event: ProgressEvent): void;
  }
) => Promise<CoreMethodMap["applySync"]["output"]>;

export interface DesktopRuntimeHost {
  dispatch<M extends DesktopRuntimeMethod>(
    request: CoreRequestEnvelope<M>,
    control?: DesktopRuntimeDispatchControl
  ): Promise<CoreResponseEnvelope<M>>;
}

export function createDesktopRuntimeHost(
  profiles: DesktopProfileRepository,
  testApplyInvoker?: DesktopRuntimeTestApplyInvoker
): DesktopRuntimeHost {
  const core = createCoreFacade({
    resolveProfile: async (selector) => profiles.resolve(selector)
  });

  return Object.freeze({
    async dispatch<M extends DesktopRuntimeMethod>(
      request: CoreRequestEnvelope<M>,
      control: DesktopRuntimeDispatchControl = {}
    ): Promise<CoreResponseEnvelope<M>> {
      let operationId: string | undefined;
      try {
        const handler = core[request.method] as (
          input: CoreMethodMap[M]["input"],
          hostControl?: {
            signal?: AbortSignal;
            onOperationStarted?(value: {
              operationId: string;
              operation: "sync" | "switch" | "restore";
            }): void;
            onProgress?(event: ProgressEvent): void;
          }
        ) => Promise<CoreMethodMap[M]["output"]>;
        const hostControl: Parameters<DesktopRuntimeTestApplyInvoker>[2] = {
          ...(control.signal ? { signal: control.signal } : {}),
          onOperationStarted(value) {
            operationId = value.operationId;
            control.onOperationStarted?.({
              protocolVersion: 1,
              requestId: request.requestId,
              operationId: value.operationId,
              event: "operation-started",
              operation: value.operation
            });
          },
          onProgress(progress) {
            if (!operationId) return;
            control.onProgress?.({
              protocolVersion: 1,
              requestId: request.requestId,
              operationId,
              event: "progress",
              progress
            });
          }
        };
        const result: CoreMethodMap[M]["output"] = testApplyInvoker
          && (request.method === "applySync"
            || request.method === "applySwitch"
            || request.method === "applyRestore")
          ? await testApplyInvoker(
              request.method,
              request.payload as CoreMethodMap["applySync"]["input"],
              hostControl
            ) as CoreMethodMap[M]["output"]
          : await handler(request.payload, hostControl);
        const resultOperationId = result !== null
          && typeof result === "object"
          && "operationId" in result
          && typeof result.operationId === "string"
          ? result.operationId
          : operationId;
        return createCoreSuccessEnvelope(request, result, resultOperationId);
      } catch (error) {
        const dto = error instanceof Error
          && error.name === "AbortError"
          && (error as Error & { code?: unknown }).code === "ABORT_ERR"
          ? createPublicCoreErrorDto("OPERATION_CANCELLED", { operationId })
          : sanitizePublicCoreErrorDto(error);
        return createCoreFailureEnvelope(request, dto, operationId);
      }
    }
  });
}
