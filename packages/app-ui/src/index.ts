export { AppUi } from "./App.js";
export { APP_ROUTES, type AppRoute } from "./routes.js";
export { createAppI18n, resources, resourcesHaveMatchingKeys } from "./i18n.js";
export { profileSchema, restoreSchema, switchSchema, syncSchema } from "./schemas.js";
export type { AppUiProps, HostClient, HostProfile, PreferenceStore, SaveProfileInput } from "./types.js";

export const APP_UI_MIGRATION_STATE = "shared-ui-c5" as const;
