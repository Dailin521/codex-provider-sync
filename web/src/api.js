const bootstrap = window.__CODEX_PROVIDER_SYNC__ ?? {};

export async function apiRequest(path, body = {}) {
  const response = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Codex-Provider-Token": bootstrap.apiToken ?? ""
    },
    body: JSON.stringify(body)
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error ?? `Request failed with HTTP ${response.status}.`);
  }
  return payload;
}

export async function getActivity(after = 0) {
  const response = await fetch(`/api/activity?after=${after}`, {
    headers: { "X-Codex-Provider-Token": bootstrap.apiToken ?? "" }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error ?? `Activity request failed with HTTP ${response.status}.`);
  }
  return payload;
}

export async function getHistory(body = {}) {
  return apiRequest("/api/history", body);
}

export async function getHistorySession(body = {}) {
  return apiRequest("/api/history/session", body);
}
