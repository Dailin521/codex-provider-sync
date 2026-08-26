export const APP_ROUTES = [
  "overview",
  "sync",
  "switch-provider",
  "backups-restore",
  "history",
  "profiles",
  "diagnostics",
  "settings"
] as const;

export type AppRoute = typeof APP_ROUTES[number];
