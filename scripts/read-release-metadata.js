#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const RELEASE_TAG_PATTERN =
  /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;
const TITLE_PATTERN = /^<!-- release-title: ([^\r\n]+) -->$/gm;
const REQUIRED_ANNOUNCEMENT_MARKERS = [
  "## 📦 下载",
  "## ⬆️ 升级说明",
  "## 🛡 安全保障",
  "## ⚠️ 重要说明",
  "## 🔍 验证结果",
  "SmartScreen",
  "SHA-256",
  "`auth.json`",
  "`updated_at`",
  "`encrypted_content`",
  "回滚",
  "WSL UNC",
];

function displayPath(rootDir, targetPath) {
  return path.relative(rootDir, targetPath).replaceAll("\\", "/");
}

export function readReleaseMetadata({ rootDir, tag }) {
  if (typeof tag !== "string" || !RELEASE_TAG_PATTERN.test(tag)) {
    throw new Error(`Release tag must use the form v<semver>; received ${JSON.stringify(tag)}.`);
  }

  const relativeBodyPath = `docs/release-notes/${tag}-zh.md`;
  const bodyPath = path.join(rootDir, ...relativeBodyPath.split("/"));
  if (!fs.existsSync(bodyPath)) {
    throw new Error(`Chinese release announcement is missing: ${relativeBodyPath}`);
  }

  const body = fs.readFileSync(bodyPath, "utf8");
  const titleMatches = [...body.matchAll(TITLE_PATTERN)];
  if (titleMatches.length !== 1) {
    throw new Error(
      `${relativeBodyPath} must contain exactly one <!-- release-title: ... --> line.`,
    );
  }

  const title = titleMatches[0][1].trim();
  if (!title.startsWith(`${tag} - `) || title.length > 120) {
    throw new Error(
      `${relativeBodyPath} release title must start with ${JSON.stringify(`${tag} - `)} and be at most 120 characters.`,
    );
  }
  if (body.length < 200) {
    throw new Error(`${relativeBodyPath} is too short to be a complete release announcement.`);
  }
  if (/\b(?:TODO|TBD)\b/.test(body)) {
    throw new Error(`${relativeBodyPath} still contains a TODO or TBD placeholder.`);
  }
  const missingMarkers = REQUIRED_ANNOUNCEMENT_MARKERS.filter(
    (marker) => !body.includes(marker),
  );
  if (missingMarkers.length > 0) {
    throw new Error(
      `${relativeBodyPath} is missing required release or safety content: ${missingMarkers.join(", ")}`,
    );
  }

  return {
    tag,
    title,
    body,
    bodyPath,
    relativeBodyPath: displayPath(rootDir, bodyPath),
  };
}

function parseArguments(argumentsList) {
  if (argumentsList.length < 2 || argumentsList[0] !== "--tag" || !argumentsList[1]) {
    throw new Error(
      "Usage: node scripts/read-release-metadata.js --tag v<semver> [--github-output <path>]",
    );
  }

  const options = { tag: argumentsList[1], githubOutput: null };
  for (let index = 2; index < argumentsList.length; index += 2) {
    if (argumentsList[index] !== "--github-output" || !argumentsList[index + 1]) {
      throw new Error(
        "Usage: node scripts/read-release-metadata.js --tag v<semver> [--github-output <path>]",
      );
    }
    options.githubOutput = argumentsList[index + 1];
  }
  return options;
}

function main() {
  try {
    const { tag, githubOutput } = parseArguments(process.argv.slice(2));
    const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
    const result = readReleaseMetadata({
      rootDir: path.resolve(scriptDirectory, ".."),
      tag,
    });

    if (githubOutput) {
      fs.appendFileSync(
        githubOutput,
        `release_title=${result.title}\nrelease_body_path=${result.relativeBodyPath}\n`,
        "utf8",
      );
    } else {
      console.log(
        JSON.stringify(
          {
            tag: result.tag,
            title: result.title,
            bodyPath: result.relativeBodyPath,
          },
          null,
          2,
        ),
      );
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

const isDirectInvocation =
  process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;

if (isDirectInvocation) {
  main();
}
