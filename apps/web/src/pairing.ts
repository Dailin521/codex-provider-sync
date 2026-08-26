import type { HostClient, HostProfile, PreferenceStore, SaveProfileInput } from "@codex-provider-sync/app-ui";

const DEVICE_STORAGE_KEY = "cps.web.deviceCredential";
const LOCALE_STORAGE_KEY = "cps.preference.locale";
const THEME_STORAGE_KEY = "cps.preference.theme";

function credential(): string {
  return globalThis.localStorage.getItem(DEVICE_STORAGE_KEY) ?? "";
}

async function jsonResponse(response: Response): Promise<Record<string, unknown>> {
  const payload = await response.json().catch(() => ({}));
  return payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload as Record<string, unknown>
    : {};
}

function safeHostError(payload: Record<string, unknown>, fallback: string): Error {
  const code = typeof payload.code === "string" ? payload.code : "HOST_REQUEST_FAILED";
  return Object.assign(new Error(`${fallback} (${code})`), { code });
}

export async function initializePairing(): Promise<string | null> {
  const fragment = new URLSearchParams(globalThis.location.hash.replace(/^#/, ""));
  const pairingToken = fragment.get("pair");
  if (!pairingToken) return credential() || null;
  globalThis.history.replaceState(null, "", `${globalThis.location.pathname}${globalThis.location.search}`);
  const response = await globalThis.fetch("/api/pair", {
    method: "POST",
    redirect: "error",
    credentials: "same-origin",
    headers: { "X-Codex-Provider-Pairing": pairingToken }
  });
  const payload = await jsonResponse(response);
  const deviceCredential = typeof payload.deviceCredential === "string" ? payload.deviceCredential : "";
  if (!response.ok || !deviceCredential) return null;
  globalThis.localStorage.setItem(DEVICE_STORAGE_KEY, deviceCredential);
  return deviceCredential;
}

export function createAuthenticatedFetch(deviceCredential: string): typeof globalThis.fetch {
  return async (input, init = {}) => {
    const response = await globalThis.fetch(input, {
      ...init,
      headers: { ...Object.fromEntries(new Headers(init.headers).entries()), "X-Codex-Provider-Device": deviceCredential }
    });
    if (response.status === 403) {
      const payload = await jsonResponse(response.clone());
      if (payload.code === "PAIRING_REQUIRED") {
        globalThis.localStorage.removeItem(DEVICE_STORAGE_KEY);
        globalThis.dispatchEvent(new CustomEvent("cps:pairing-required"));
      }
    }
    return response;
  };
}

export function createHostClient(deviceCredential: string): HostClient {
  const fetch = createAuthenticatedFetch(deviceCredential);
  const headers = { "Content-Type": "application/json" };
  const getProfiles = async (signal?: AbortSignal): Promise<HostProfile[]> => {
    const response = await fetch("/api/profiles", { credentials: "same-origin", redirect: "error", signal });
    const payload = await jsonResponse(response);
    if (!response.ok || !Array.isArray(payload.profiles)) throw safeHostError(payload, "Unable to load profiles");
    return payload.profiles as unknown as HostProfile[];
  };
  return {
    listProfiles: getProfiles,
    async saveProfile(input: SaveProfileInput, signal?: AbortSignal): Promise<HostProfile> {
      const response = await fetch("/api/profiles/save", { method: "POST", credentials: "same-origin", redirect: "error", headers, body: JSON.stringify(input), signal });
      const payload = await jsonResponse(response);
      if (!response.ok || !payload.profile) throw safeHostError(payload, "Unable to save profile");
      return payload.profile as unknown as HostProfile;
    },
    async deleteProfile(profileId: string, profileRevision: string, signal?: AbortSignal): Promise<void> {
      const response = await fetch("/api/profiles/delete", { method: "POST", credentials: "same-origin", redirect: "error", headers, body: JSON.stringify({ profileId, profileRevision }), signal });
      const payload = await jsonResponse(response);
      if (!response.ok) throw safeHostError(payload, "Unable to delete profile");
    },
    async forgetBrowser(): Promise<void> {
      try {
        await fetch("/api/access/forget", { method: "POST", credentials: "same-origin", redirect: "error", headers, body: "{}" });
      } finally {
        globalThis.localStorage.removeItem(DEVICE_STORAGE_KEY);
      }
    }
  };
}

export const preferenceStore: PreferenceStore = {
  getLocale() {
    const value = globalThis.localStorage.getItem(LOCALE_STORAGE_KEY);
    return value === "zh-CN" || value === "en" ? value : null;
  },
  setLocale(locale) { globalThis.localStorage.setItem(LOCALE_STORAGE_KEY, locale); },
  getTheme() {
    const value = globalThis.localStorage.getItem(THEME_STORAGE_KEY);
    return value === "system" || value === "light" || value === "dark" ? value : null;
  },
  setTheme(theme) { globalThis.localStorage.setItem(THEME_STORAGE_KEY, theme); }
};

export async function forgetBrowser(host: HostClient): Promise<void> {
  await host.forgetBrowser?.();
  globalThis.location.reload();
}
