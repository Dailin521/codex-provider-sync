import {
  applyRepair,
  applyRestore,
  applySwitch,
  applySync
} from "../../../src/public-api.js";

/**
 * Test-build-only bridge into the legacy internal fault hook. The package is
 * private, remains a desktop devDependency, and is excluded from production
 * Electron bundles and the root npm tarball.
 */
export function applyPreparedDesktopOperationForTest(
  method,
  input,
  control,
  faultInjector
) {
  const apply = method === "applySync"
    ? applySync
    : method === "applySwitch"
      ? applySwitch
      : method === "applyRepair"
        ? applyRepair
      : applyRestore;
  return apply(input, { ...control, faultInjector });
}
