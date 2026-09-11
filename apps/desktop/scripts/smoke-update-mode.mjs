// Test expectations come from the audited container, never from the app's response.
export function expectedContainerUpdateMode({ updaterAuthorized, containerKind }) {
  return updaterAuthorized && containerKind === "nsis" ? "installer" : "manual";
}

export function parseSmokeUpdateMode(value = "manual") {
  if (value !== "manual" && value !== "installer") {
    throw new Error("CPS_EXPECTED_UPDATE_MODE must be manual or installer.");
  }
  return value;
}
