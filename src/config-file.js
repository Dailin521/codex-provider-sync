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

export function readProviderBlock(configText, provider) {
  const escapedProvider = provider.replaceAll(".", "\\.");
  const headerRegex = new RegExp(`^\\[model_providers\\.${escapedProvider}]\\s*$`, "m");
  const headerMatch = headerRegex.exec(configText);
  if (!headerMatch) {
    return null;
  }

  const blockStart = headerMatch.index + headerMatch[0].length;
  const rest = configText.slice(blockStart);
  const nextHeaderMatch = /^\[[^\]]+]\s*$/m.exec(rest);
  const blockEnd = nextHeaderMatch ? blockStart + nextHeaderMatch.index : configText.length;
  return configText.slice(blockStart, blockEnd);
}

export function providerBlockHasCustomEndpoint(configText, provider) {
  const block = readProviderBlock(configText, provider);
  if (!block) {
    return false;
  }

  return splitLines(block).some((line) => {
    const trimmed = line.trim();
    return trimmed
      && !trimmed.startsWith("#")
      && /^base_url\s*=\s*("[^"]+"|'[^']+')\s*$/.test(trimmed);
  });
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
