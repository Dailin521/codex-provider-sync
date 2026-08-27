export { AppUi } from "./App.js";
export { APP_ROUTES, type AppRoute } from "./routes.js";
export { createAppI18n, resources, resourcesHaveMatchingKeys } from "./i18n.js";
export { profileSchema, restoreSchema, switchSchema, syncSchema } from "./schemas.js";
export {
  FULL_APP_UI_CAPABILITIES,
  READ_ONLY_APP_UI_CAPABILITIES,
  SYNC_SWITCH_APP_UI_CAPABILITIES,
  DESKTOP_C8_APP_UI_CAPABILITIES
} from "./types.js";
export type {
  AppUiCapabilities,
  AppUiProps,
  HostClient,
  HostDiagnosticsExportResult,
  HostProfile,
  HostUpdateStatus,
  PreferenceStore,
  SaveProfileInput
} from "./types.js";

export const APP_UI_MIGRATION_STATE = "shared-ui-c5" as const;
