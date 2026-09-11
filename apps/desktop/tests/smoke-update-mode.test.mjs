import assert from "node:assert/strict";
import test from "node:test";
import { expectedContainerUpdateMode, parseSmokeUpdateMode } from "../scripts/smoke-update-mode.mjs";

test("only an authorized NSIS container expects the installed updater", () => {
  for (const updaterAuthorized of [false, true]) {
    for (const containerKind of ["nsis", "zip", "dmg", "appimage", "deb"]) {
      assert.equal(expectedContainerUpdateMode({ updaterAuthorized, containerKind }),
        updaterAuthorized && containerKind === "nsis" ? "installer" : "manual");
    }
  }
});

test("direct unpacked smoke defaults to manual; invalid expectations fail closed", () => {
  assert.equal(parseSmokeUpdateMode(), "manual");
  assert.equal(parseSmokeUpdateMode("installer"), "installer");
  assert.equal(parseSmokeUpdateMode("manual"), "manual");
  for (const value of ["", "auto", "installed", null]) {
    assert.throws(() => parseSmokeUpdateMode(value), /CPS_EXPECTED_UPDATE_MODE/);
  }
});
