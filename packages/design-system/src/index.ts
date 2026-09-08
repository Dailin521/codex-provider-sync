export const THEME_MODES = ["system", "light", "dark"] as const;
export type ThemeMode = typeof THEME_MODES[number];

export const SUPPORTED_LOCALES = ["zh-CN", "en"] as const;
export type SupportedLocale = typeof SUPPORTED_LOCALES[number];

export const DESIGN_SYSTEM_MIGRATION_STATE = "tokens-and-primitives-c5" as const;
