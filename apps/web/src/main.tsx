import { AppUi } from "@codex-provider-sync/app-ui";
import { HttpCoreClient } from "@codex-provider-sync/core-client";
import React from "react";
import { createRoot } from "react-dom/client";

import { createAuthenticatedFetch, createHostClient, forgetBrowser, initializePairing, preferenceStore } from "./pairing.js";
import "./styles.css";

const root = createRoot(document.getElementById("root")!);
const deviceCredential = await initializePairing();

if (!deviceCredential) {
  root.render(
    <React.StrictMode>
      <main className="grid min-h-screen place-items-center bg-[var(--surface)] p-6 text-[var(--text)]">
        <section className="max-w-lg rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)] p-8 text-center shadow-xl">
          <h1 className="text-2xl font-bold">Codex Provider Sync</h1>
          <p className="mt-3 text-sm leading-6 text-[var(--muted)]">This browser is not paired. Run <code>codex-provider web</code> again and open the new one-time link.</p>
        </section>
      </main>
    </React.StrictMode>
  );
} else {
  const host = createHostClient(deviceCredential);
  const authenticatedFetch = createAuthenticatedFetch(deviceCredential);
  const core = new HttpCoreClient({
    baseUrl: globalThis.location.origin,
    fetch: authenticatedFetch
  });
  globalThis.addEventListener("cps:pairing-required", () => globalThis.location.reload(), { once: true });
  root.render(
    <React.StrictMode>
      <AppUi
        core={core}
        host={host}
        initialLocale={globalThis.navigator.language.toLowerCase().startsWith("zh") ? "zh-CN" : "en"}
        initialTheme="system"
        onForgetBrowser={() => forgetBrowser(host)}
        preferences={preferenceStore}
      />
    </React.StrictMode>
  );
}
