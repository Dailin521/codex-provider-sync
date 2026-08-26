import {
  createCoreFailureEnvelope,
  createCoreSuccessEnvelope,
  sanitizePublicCoreErrorDto,
  type CoreMethodMap,
  type CoreRequestEnvelope,
  type CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import { createCoreFacade } from "@codex-provider-sync/core";
import type { DesktopReadMethod } from "@codex-provider-sync/core-client";

import type { DesktopProfileRepository } from "../profiles/repository.js";

export interface DesktopRuntimeHost {
  dispatch<M extends DesktopReadMethod>(
    request: CoreRequestEnvelope<M>
  ): Promise<CoreResponseEnvelope<M>>;
}

export function createDesktopRuntimeHost(
  profiles: DesktopProfileRepository
): DesktopRuntimeHost {
  const core = createCoreFacade({
    resolveProfile: async (selector) => profiles.resolve(selector)
  });

  return Object.freeze({
    async dispatch<M extends DesktopReadMethod>(
      request: CoreRequestEnvelope<M>
    ): Promise<CoreResponseEnvelope<M>> {
      try {
        const handler = core[request.method] as (
          input: CoreMethodMap[M]["input"]
        ) => Promise<CoreMethodMap[M]["output"]>;
        const result = await handler(request.payload);
        return createCoreSuccessEnvelope(request, result);
      } catch (error) {
        return createCoreFailureEnvelope(request, sanitizePublicCoreErrorDto(error));
      }
    }
  });
}
