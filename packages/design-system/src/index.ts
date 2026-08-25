export const THEME_MODES = ["system", "light", "dark"] as const;
export type ThemeMode = typeof THEME_MODES[number];

export const SUPPORTED_LOCALES = ["zh-CN", "en"] as const;
export type SupportedLocale = typeof SUPPORTED_LOCALES[number];

export const DESIGN_SYSTEM_MIGRATION_STATE = "tokens-only-c4" as const;

// React components, Tailwind output and the checked-in Radix/shadcn-style
// component set are C5 deliverables. C4 only establishes the platform-neutral
// token and locale ownership boundary.
