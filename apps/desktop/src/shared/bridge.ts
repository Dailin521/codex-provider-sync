import type {
  CoreRequestEnvelope,
  CoreResponseEnvelope
} from "@codex-provider-sync/contracts";
import type {
  DesktopCoreBridge,
  DesktopReadMethod
} from "@codex-provider-sync/core-client";

import type { DesktopProfileListResponse } from "./profile-types.js";

export interface DesktopBridgeApi {
  readonly version: 1;
  readonly core: DesktopCoreBridge;
  readonly profiles: {
    list(): Promise<DesktopProfileListResponse>;
  };
  readonly test?: {
    crashRuntime(): Promise<{ crashed: boolean }>;
    requestRaw(envelope: CoreRequestEnvelope<DesktopReadMethod>): Promise<CoreResponseEnvelope>;
  };
}
