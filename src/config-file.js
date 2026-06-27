import fs from "node:fs/promises";

import { DEFAULT_PROVIDER } from "./constants.js";

function splitLines(text) {
  return text.split(/\r?\n/);
}

function escapeTomlString(value) {
  return value.replaceAll("\\", "\\\\").replaceAll("\"", "\\\"");
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

// Return the root-level `model` value from config.toml, or null
// when the file does not declare one. Used by `runSync`,
// `runSwitch`, and `runWatch` to keep the per-thread model rewrite
// aligned with the active top-level model. Single source of truth
// for the regex so we do not end up with three slightly different
// parsers disagreeing about what counts as a valid model name.
export function readRootModelFromConfigText(configText) {
  const lines = splitLines(configText);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    if (trimmed.startsWith("[")) {
      // Stop at the first table header; `model` is only valid at
      // the root level, not inside a `[model_providers.X]` section.
      break;
    }
    const match = trimmed.match(/^model\s*=\s*"([^"]+)"\s*$/);
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
      // Preserve the input's trailing-newline state so we do not
      // silently rewrite `\n\n\n` to `\n\n` (or `\n` to nothing)
      // — the previous `.replace(/\n\n$/, "\n")` only handled the
      // two-trailing-newline case, which is now caught earlier
      // by the helper.
      return finalizeTrailingNewline(lines.join("\n"), configText);
    }
    insertIndex = index + 1;
  }

  lines.splice(insertIndex, 0, `model_provider = "${escapeTomlString(provider)}"`);
  const nextText = lines.join("\n");
  return finalizeTrailingNewline(nextText, configText);
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
      return finalizeTrailingNewline(lines.join("\n"), configText);
    }
    insertIndex = index + 1;
  }

  lines.splice(insertIndex, 0, `model = "${escapeTomlString(model)}"`);
  const nextText = lines.join("\n");
  return finalizeTrailingNewline(nextText, configText);
}

// Match the input's trailing-newline state so a config that
// ended in `\n\n` (rare but happens after manual edits) is not
// silently collapsed to a single `\n` by the surrounding logic.
// Returning the joined lines unchanged when the input had no
// trailing newline also matches the C# `ConfigFileService`
// implementation and avoids surprising the user with byte-level
// diffs to their config.
function finalizeTrailingNewline(joinedLines, originalConfigText) {
  if (originalConfigText.endsWith("\n")) {
    return joinedLines.endsWith("\n") ? joinedLines : `${joinedLines}\n`;
  }
  return joinedLines.endsWith("\n") ? joinedLines.slice(0, -1) : joinedLines;
}

export async function writeConfigText(configPath, configText) {
  await fs.writeFile(configPath, configText, "utf8");
}
