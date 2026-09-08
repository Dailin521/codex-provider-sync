export { AppUi } from "./App.js";
export { APP_ROUTES, type AppRoute } from "./routes.js";
export { createAppI18n, resources, resourcesHaveMatchingKeys } from "./i18n.js";
export {
  DEFAULT_BACKUP_RETENTION_COUNT,
  profileSchema,
  restoreSchema,
  switchSchema,
  syncSchema
} from "./schemas.js";
export {
  FULL_APP_UI_CAPABILITIES,
  READ_ONLY_APP_UI_CAPABILITIES,
  SYNC_SWITCH_APP_UI_CAPABILITIES,
  DESKTOP_C8_APP_UI_CAPABILITIES
} from "./types.js";
export type {
  AppUiCapabilities,
  AppUiProps,
  AppUiSurface,
  HostClient,
  HostDiagnosticsExportResult,
  HostProfile,
  OperationLogEntry,
  OperationLogPage,
  OperationLogStatus,
  HostUpdateStatus,
  PreferenceStore,
  SaveProfileInput
} from "./types.js";

export const APP_UI_MIGRATION_STATE = "shared-ui-c5" as const;
export { createProjectAliasPreferences } from "./features/history/project-alias-preferences.js";
export { createBackupPreferences } from "./shared/backup-preferences.js";
