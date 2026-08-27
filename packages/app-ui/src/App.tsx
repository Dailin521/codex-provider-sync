import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect, useLayoutEffect, useState } from "react";
import { I18nextProvider } from "react-i18next";

import { AppContent } from "./app/AppContent.js";
import { AppErrorBoundary } from "./app/AppErrorBoundary.js";
import { createAppI18n } from "./i18n.js";
import { APP_ROUTES } from "./routes.js";
import type { AppUiProps } from "./types.js";
import { ToastProvider } from "./ui.js";

export function AppUi(props: AppUiProps) {
  const requestedLocale = props.preferences.getLocale() ?? props.initialLocale;
  const requestedTheme = props.preferences.getTheme() ?? props.initialTheme;
  const [i18n, setI18n] = useState<Awaited<ReturnType<typeof createAppI18n>> | null>(null);
  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: {
      queries: { retry: 1, refetchOnWindowFocus: false },
      mutations: { retry: false }
    }
  }));

  useEffect(() => {
    let active = true;
    void createAppI18n(requestedLocale).then((instance) => {
      if (active) setI18n(instance);
    });
    return () => {
      active = false;
      queryClient.clear();
    };
  }, [props.initialTheme, props.preferences, queryClient, requestedLocale]);

  useLayoutEffect(() => {
    document.documentElement.dataset.theme = requestedTheme;
  }, [requestedTheme]);

  if (!i18n) {
    return (
      <div className="grid min-h-screen place-items-center bg-[var(--surface)] text-[var(--text)]">
        {requestedLocale === "zh-CN" ? "正在加载…" : "Loading…"}
      </div>
    );
  }

  return (
    <I18nextProvider i18n={i18n}>
      <AppErrorBoundary locale={() => i18n.language}>
        <QueryClientProvider client={queryClient}>
          <ToastProvider><AppContent props={props} /></ToastProvider>
        </QueryClientProvider>
      </AppErrorBoundary>
    </I18nextProvider>
  );
}

export { APP_ROUTES };
