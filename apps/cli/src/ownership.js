// C4 establishes package ownership without moving the compatibility binary.
// The published root package must continue to execute src/cli.js on Node 16.
export const CLI_MIGRATION_STATE = Object.freeze({
  compatibilityEntrypoint: "src/cli.js",
  owner: "apps/cli",
  implementationMoved: false
});
