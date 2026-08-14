export const MINIMUM_NODE_VERSION = "16.20.2";

function parseVersion(value) {
  const match = String(value).replace(/^v/, "").match(/^(\d+)\.(\d+)\.(\d+)/);
  return match ? match.slice(1).map((part) => Number.parseInt(part, 10)) : null;
}

function versionAtLeast(current, minimum) {
  for (let index = 0; index < minimum.length; index += 1) {
    if (current[index] > minimum[index]) return true;
    if (current[index] < minimum[index]) return false;
  }
  return true;
}

export function getUnsupportedNodeVersionMessage(nodeVersion = process.versions.node) {
  const current = parseVersion(nodeVersion);
  const minimum = parseVersion(MINIMUM_NODE_VERSION);
  if (current && minimum && versionAtLeast(current, minimum)) {
    return null;
  }

  const displayVersion = String(nodeVersion).startsWith("v") ? String(nodeVersion) : `v${nodeVersion}`;
  return `codex-provider-sync requires Node.js ${MINIMUM_NODE_VERSION} or newer. `
    + `Current Node.js version: ${displayVersion}. `
    + "Please upgrade Node.js, then reinstall or rerun codex-provider.";
}

export function assertSupportedNodeVersion() {
  const message = getUnsupportedNodeVersionMessage();
  if (message) {
    throw new Error(message);
  }
}
