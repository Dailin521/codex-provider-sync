import test from "node:test";
import assert from "node:assert/strict";

import {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readCurrentProviderFromConfigText,
  readSqliteHomeFromConfigText,
  setRootProviderInConfigText
} from "../src/config-file.js";

test("readCurrentProviderFromConfigText falls back to implicit openai", () => {
  const input = `
# comment
sandbox_mode = "danger-full-access"

[features]
apps = true
`;

  assert.deepEqual(readCurrentProviderFromConfigText(input), {
    provider: "openai",
    implicit: true
  });
});

test("setRootProviderInConfigText inserts root-level model_provider before first table", () => {
  const input = `# comment
sandbox_mode = "danger-full-access"

[features]
apps = true
`;

  const next = setRootProviderInConfigText(input, "apigather");
  assert.match(next, /^# comment\nsandbox_mode = "danger-full-access"\n\nmodel_provider = "apigather"\n\[features]/);
});

test("setRootProviderInConfigText updates existing root-level model_provider", () => {
  const input = `model_provider = "openai"\nsandbox_mode = "danger-full-access"\n`;
  const next = setRootProviderInConfigText(input, "newapi");
  assert.equal(next, `model_provider = "newapi"\nsandbox_mode = "danger-full-access"\n`);
});

test("provider declarations include openai and custom tables", () => {
  const input = `
[model_providers.apigather]
base_url = "https://example.com"

[model_providers.newapi]
base_url = "https://example.org"
`;

  assert.deepEqual(listConfiguredProviderIds(input), ["apigather", "newapi", "openai"]);
  assert.equal(configDeclaresProvider(input, "apigather"), true);
  assert.equal(configDeclaresProvider(input, "missing"), false);
});

test("readSqliteHomeFromConfigText reads only root-level sqlite_home", () => {
  assert.equal(readSqliteHomeFromConfigText(`
model_provider = "openai"
sqlite_home = "/tmp/codex-state"

[features]
sqlite_home = "/ignored"
`), "/tmp/codex-state");

  assert.equal(readSqliteHomeFromConfigText(`
[features]
sqlite_home = "/ignored"
`), null);
});
