const PATCH_VERSION = "1\\.0\\.(?:0|[1-9]\\d*)";
const CANDIDATE_VERSION = new RegExp(`^${PATCH_VERSION}-(?:alpha|beta|rc)\\.(?:0|[1-9]\\d*)$`);
const STABLE_WINDOWS_VERSION = new RegExp(`^${PATCH_VERSION}$`);

/** Stable Windows artifacts require an explicit manual-release channel, not updater authorization. */
export function assertDesktopArtifactVersion({ version, target, releaseChannel }) {
  if (releaseChannel === "stable-manual") {
    if (STABLE_WINDOWS_VERSION.test(version || "") && target === "windows-x64") return;
    throw new Error("Manual stable release requires a strict Windows x64 1.0.x version.");
  }
  if (releaseChannel && releaseChannel !== "rc") throw new Error("Unsupported release channel.");
  if (!CANDIDATE_VERSION.test(version || "")) throw new Error("Unsupported v1 candidate version.");
}
