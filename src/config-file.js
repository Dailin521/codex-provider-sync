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

export function readSqliteHomeFromConfigText(configText) {
  const lines = splitLines(configText);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    if (trimmed.startsWith("[")) {
      break;
    }
    const match = trimmed.match(/^sqlite_home\s*=\s*"([^"]+)"\s*$/);
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

export async function writeConfigText(configPath, configText) {
  await fs.writeFile(configPath, configText, "utf8");
}
