import test from "node:test";
import assert from "node:assert/strict";

import {
  configDeclaresProvider,
  listConfiguredProviderIds,
  readCurrentProviderFromConfigText,
  readProviderModel,
  setRootModelInConfigText,
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

test("readProviderModel returns the model field from a [model_providers.X] section", () => {
  const input = `
[model_providers.codexzh]
name = "codexzh"
model = "gpt-5.4"
base_url = "https://api.codexzh.com/v1"

[model_providers.longcat]
name = "longcat"
model = "LongCat-2.0-Preview"
base_url = "https://api.longcat.chat/openai/v1"
`;

  assert.equal(readProviderModel(input, "codexzh"), "gpt-5.4");
  assert.equal(readProviderModel(input, "longcat"), "LongCat-2.0-Preview");
});

test("readProviderModel returns null when the section is missing or has no model field", () => {
  const input = `
[model_providers.codexzh]
name = "codexzh"
base_url = "https://api.codexzh.com/v1"
`;

  assert.equal(readProviderModel(input, "codexzh"), null);
  assert.equal(readProviderModel(input, "longcat"), null);
  assert.equal(readProviderModel(input, "openai"), null);
});

test("readProviderModel handles unusual provider names without regex injection", () => {
  const input = `
[model_providers.weird.name-1]
model = "X"
`;
  // Should not throw on regex metacharacters in provider id (dots, dashes, etc.)
  assert.equal(readProviderModel(input, "weird.name-1"), "X");
});

test("setRootModelInConfigText inserts a new root-level model before the first table", () => {
  const input = `model_provider = "codexzh"\n\n[features]\napps = true\n`;
  const next = setRootModelInConfigText(input, "LongCat-2.0-Preview");
  // Inserts at the position where [features] would have been, preserving
  // the blank line that was already there. The output's trailing
  // newline exactly matches the input's trailing newline so the
  // config file's byte-level shape is preserved.
  assert.equal(
    next,
    `model_provider = "codexzh"\n\nmodel = "LongCat-2.0-Preview"\n[features]\napps = true\n`
  );
});

test("setRootModelInConfigText updates an existing root-level model", () => {
  const input = `model_provider = "codexzh"\nmodel = "gpt-5.4-mini"\n\n[features]\napps = true\n`;
  const next = setRootModelInConfigText(input, "gpt-5.4");
  assert.equal(
    next,
    `model_provider = "codexzh"\nmodel = "gpt-5.4"\n\n[features]\napps = true\n`
  );
});

test("setRootModelInConfigText escapes backslashes and quotes", () => {
  const input = `model = "old"\n`;
  const next = setRootModelInConfigText(input, `weird"path\\name`);
  assert.equal(next, `model = "weird\\"path\\\\name"\n`);
});

test("setRootProviderInConfigText + setRootModelInConfigText produces a coherent root block", () => {
  const input = `model_provider = "codexzh"\nmodel = "gpt-5.4-mini"\n\n[model_providers.longcat]\nname = "longcat"\nmodel = "LongCat-2.0-Preview"\n`;
  let next = setRootProviderInConfigText(input, "longcat");
  next = setRootModelInConfigText(next, readProviderModel(input, "longcat"));
  assert.equal(
    next,
    `model_provider = "longcat"\nmodel = "LongCat-2.0-Preview"\n\n[model_providers.longcat]\nname = "longcat"\nmodel = "LongCat-2.0-Preview"\n`
  );
});
