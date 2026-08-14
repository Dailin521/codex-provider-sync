const DEVICE_STORAGE_KEY = "cps.web.deviceCredential";

export class PairingRequiredError extends Error {
  constructor(message = "此浏览器需要重新配对。请重新运行 codex-provider web。") {
    super(message);
    this.name = "PairingRequiredError";
  }
}

export function hasDeviceCredential() {
  return Boolean(window.localStorage.getItem(DEVICE_STORAGE_KEY));
}

export async function initializeAccess() {
  const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  const pairingToken = fragment.get("pair");
  if (!pairingToken) return hasDeviceCredential();

  const response = await fetch("/api/pair", {
    method: "POST",
    headers: { "X-Codex-Provider-Pairing": pairingToken }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || !payload.deviceCredential) {
    throw new PairingRequiredError(payload.error ?? "配对链接无效、已过期或已被使用。请重新运行 codex-provider web。");
  }
  window.localStorage.setItem(DEVICE_STORAGE_KEY, payload.deviceCredential);
  window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
  return true;
}

function deviceHeaders() {
  return { "X-Codex-Provider-Device": window.localStorage.getItem(DEVICE_STORAGE_KEY) ?? "" };
}

async function parseResponse(response, fallback) {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (payload.code === "PAIRING_REQUIRED") {
      window.dispatchEvent(new CustomEvent("cps:pairing-required", { detail: payload.error }));
      throw new PairingRequiredError(payload.error);
    }
    throw new Error(payload.error ?? fallback);
  }
  return payload;
}

export async function apiRequest(path, body = {}, { signal } = {}) {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...deviceHeaders()
    },
    body: JSON.stringify(body),
    signal
  });
  return parseResponse(response, `Request failed with HTTP ${response.status}.`);
}

export async function getActivity(after = 0) {
  const response = await fetch(`/api/activity?after=${after}`, {
    headers: deviceHeaders()
  });
  return parseResponse(response, `Activity request failed with HTTP ${response.status}.`);
}

export async function getProfiles() {
  const response = await fetch("/api/profiles", { headers: deviceHeaders() });
  return parseResponse(response, `Profile request failed with HTTP ${response.status}.`);
}

export async function forgetThisBrowser() {
  try {
    await apiRequest("/api/access/forget");
  } finally {
    window.localStorage.removeItem(DEVICE_STORAGE_KEY);
  }
}

export async function getHistory(body = {}, options = {}) {
  return apiRequest("/api/history", body, options);
}

export async function getHistorySession(body = {}, options = {}) {
  return apiRequest("/api/history/session", body, options);
}
