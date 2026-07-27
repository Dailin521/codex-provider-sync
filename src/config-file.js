import fs from "node:fs/promises";

import { DEFAULT_PROVIDER } from "./constants.js";

function splitLines(text) {
  return text.split(/\r?\n/);
}

function escapeTomlString(value) {
  return value.replaceAll("\\", "\\\\").replaceAll("\"", "\\\"");
}

function decodeTomlBasicString(value) {
  return value.replace(/\\(?:[btnfr"\\]|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/g, (escape) => {
    const simple = {
      "\\b": "\b",
      "\\t": "\t",
      "\\n": "\n",
      "\\f": "\f",
      "\\r": "\r",
      '\\"': '"',
      "\\\\": "\\"
    };
    if (simple[escape] !== undefined) {
      return simple[escape];
    }
    const codePoint = Number.parseInt(escape.slice(2), 16);
    return String.fromCodePoint(codePoint);
  });
}

export function readRootStringFromConfigText(configText, key) {
  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const assignment = new RegExp(`^${escapedKey}\\s*=\\s*(?:\"((?:\\\\.|[^\"\\\\])*)\"|'([^']*)')\\s*(?:#.*)?$`);
  for (const line of splitLines(configText)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    if (trimmed.startsWith("[")) {
      break;
    }
    const match = trimmed.match(assignment);
    if (match) {
      return match[1] !== undefined ? decodeTomlBasicString(match[1]) : match[2];
    }
  }
  return null;
}

export function readSqliteHomeFromConfigText(configText) {
  return readRootStringFromConfigText(configText, "sqlite_home");
}

export async function readConfigText(configPath) {
  return fs.readFile(configPath, "utf8");
}

export function readCurrentProviderFromConfigText(configText) {
  const lines = splitLines(configText);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    if (trimmed.startsWith("[")) {
      break;
    }
    const match = trimmed.match(/^model_provider\s*=\s*"([^"]+)"\s*$/);
    if (match) {
      return { provider: match[1], implicit: false };
    }
  }
  return { provider: DEFAULT_PROVIDER, implicit: true };
}

// Read the root-level `model = "..."` field from a Codex config.toml.
// We deliberately only consider the first occurrence of `model = "..."`
// before any `[section]` header. Without this guard, a config like
//
//   model_provider = "foo"
//
//   [model_providers.foo]
//   model = "gpt-4o-mini"
//
// would return `"gpt-4o-mini"` here, and the per-rollout rewrite step
// would propagate that provider-section model into every turn_context
// line — which is wrong, because the user-set root model is the value
// the Codex CLI/GUI actually use for the next turn.
//
// Returns `null` when no root-level `model` is set; callers should
// treat that as "do not touch the per-turn model field".
export function readRootModelFromConfigText(configText) {
  const lines = splitLines(configText);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    if (trimmed.startsWith("[")) {
      break;
    }
    const match = trimmed.match(/^model\s*=\s*"([^"]+)"\s*$/);
    if (match) {
      return match[1];
    }
  }
  return null;
}

export function listConfiguredProviderIds(configText) {
  const providerIds = new Set([DEFAULT_PROVIDER]);
  const regex = /^\[model_providers\.([A-Za-z0-9_.-]+)]\s*$/gm;
  for (const match of configText.matchAll(regex)) {
    providerIds.add(match[1]);
  }
  return [...providerIds].sort();
}

export function configDeclaresProvider(configText, provider) {
  return listConfiguredProviderIds(configText).includes(provider);
}

function locateProviderSection(configText, provider) {
  const lines = splitLines(configText);
  const startRegex = new RegExp(`^\\[model_providers\\.${provider.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\]\\s*$`);
  let sectionStart = -1;
  for (let index = 0; index < lines.length; index += 1) {
    if (startRegex.test(lines[index].trim())) {
      sectionStart = index;
      break;
    }
  }
  if (sectionStart === -1) {
    return null;
  }

  const sectionLines = [];
  for (let index = sectionStart + 1; index < lines.length; index += 1) {
    const trimmed = lines[index].trim();
    if (trimmed.startsWith("[")) {
      break;
    }
    sectionLines.push({ index, line: lines[index], trimmed });
  }
  return { startIndex: sectionStart, lines: sectionLines };
}

export function readProviderModel(configText, provider) {
  if (provider === DEFAULT_PROVIDER) {
    // Built-in openai provider: there is no [model_providers.openai] section,
    // so the model is whatever the root-level `model` already is. We have no
    // canonical value to pull from — return null and let the caller decide.
    return null;
  }
  const section = locateProviderSection(configText, provider);
  if (!section) {
    return null;
  }
  for (const { line } of section.lines) {
    const match = line.match(/^\s*model\s*=\s*"([^"]+)"\s*$/);
    if (match) {
      return match[1];
    }
  }
  return null;
}

export function setRootProviderInConfigText(configText, provider) {
  const lines = splitLines(configText);
  let insertIndex = lines.length;

  for (let index = 0; index < lines.length; index += 1) {
    const trimmed = lines[index].trim();
    if (!trimmed || trimmed.startsWith("#")) {
      insertIndex = index + 1;
      continue;
    }
    if (trimmed.startsWith("[")) {
      insertIndex = index;
      break;
    }
    if (/^model_provider\s*=/.test(trimmed)) {
      lines[index] = `model_provider = "${escapeTomlString(provider)}"`;
      return `${lines.join("\n")}${configText.endsWith("\n") ? "\n" : ""}`.replace(/\n\n$/, "\n");
    }
    insertIndex = index + 1;
  }

  lines.splice(insertIndex, 0, `model_provider = "${escapeTomlString(provider)}"`);
  const nextText = lines.join("\n");
  return configText.endsWith("\n") ? `${nextText}\n` : nextText;
}

export function setRootModelInConfigText(configText, model) {
  const lines = splitLines(configText);
  let insertIndex = lines.length;

  for (let index = 0; index < lines.length; index += 1) {
    const trimmed = lines[index].trim();
    if (!trimmed || trimmed.startsWith("#")) {
      insertIndex = index + 1;
      continue;
    }
    if (trimmed.startsWith("[")) {
      insertIndex = index;
      break;
    }
    if (/^model\s*=/.test(trimmed)) {
      lines[index] = `model = "${escapeTomlString(model)}"`;
      return `${lines.join("\n")}${configText.endsWith("\n") ? "\n" : ""}`.replace(/\n\n$/, "\n");
    }
    insertIndex = index + 1;
  }

  lines.splice(insertIndex, 0, `model = "${escapeTomlString(model)}"`);
  const nextText = lines.join("\n");
  return configText.endsWith("\n") ? `${nextText}\n` : nextText;
}

export async function writeConfigText(configPath, configText) {
  await fs.writeFile(configPath, configText, "utf8");
}
