export const APP_ROUTES = [
  "overview",
  "backups-restore",
  "history",
  "operation-logs",
  "profiles",
  "diagnostics",
  "settings"
] as const;

export type AppRoute = typeof APP_ROUTES[number];
