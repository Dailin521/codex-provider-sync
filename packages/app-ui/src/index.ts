import type { CoreClient } from "@codex-provider-sync/core-client";
import type { SupportedLocale, ThemeMode } from "@codex-provider-sync/design-system";

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

export interface AppShellDependencies {
  core: CoreClient;
  locale: SupportedLocale;
  theme: ThemeMode;
}

export const APP_UI_MIGRATION_STATE = "contract-only-c4" as const;

// C5 supplies React, routing, queries, forms, validation and rendered pages.
// This package intentionally has no Electron or Node dependency.
