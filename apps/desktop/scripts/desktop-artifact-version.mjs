const CANDIDATE_VERSION = /^1\.0\.0-(?:alpha|beta|rc)\.\d+$/;

/** Stable Windows artifacts require an explicit manual-release channel, not updater authorization. */
export function assertDesktopArtifactVersion({ version, target, releaseChannel }) {
  if (releaseChannel === "stable-manual") {
    if (version === "1.0.0" && target === "windows-x64") return;
    throw new Error("Manual stable release requires Windows x64 version 1.0.0.");
  }
  if (releaseChannel && releaseChannel !== "rc") throw new Error("Unsupported release channel.");
  if (!CANDIDATE_VERSION.test(version || "")) throw new Error("Unsupported v1 candidate version.");
}
