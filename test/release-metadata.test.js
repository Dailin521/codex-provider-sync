import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { readReleaseMetadata } from "../scripts/read-release-metadata.js";

function withFixture(operation) {
  const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "release-metadata-"));
  try {
    fs.mkdirSync(path.join(rootDir, "docs", "release-notes"), { recursive: true });
    return operation(rootDir);
  } finally {
    fs.rmSync(rootDir, { force: true, recursive: true });
  }
}

function writeAnnouncement(rootDir, tag, content) {
  fs.writeFileSync(
    path.join(rootDir, "docs", "release-notes", `${tag}-zh.md`),
    content,
    "utf8",
  );
}

function completeAnnouncement(tag, title, extra = "") {
  return `<!-- release-title: ${tag} - ${title} -->

${"发布内容。".repeat(50)}

## 📦 下载
SmartScreen 与 SHA-256。

## ⬆️ 升级说明
升级说明。

## 🛡 安全保障
\`auth.json\`、\`updated_at\`、\`encrypted_content\`、回滚与 WSL UNC。

## ⚠️ 重要说明
重要说明。

## 🔍 验证结果
验证结果。

${extra}
`;
}

test("reads the current repository Chinese release metadata", () => {
  const testDirectory = path.dirname(fileURLToPath(import.meta.url));
  const rootDir = path.resolve(testDirectory, "..");

  const result = readReleaseMetadata({ rootDir, tag: "v0.4.0" });

  assert.equal(result.title, "v0.4.0 - 更安全的事务化同步与自动化支持");
  assert.equal(result.relativeBodyPath, "docs/release-notes/v0.4.0-zh.md");
});

test("reads one validated release title and body path", () =>
  withFixture((rootDir) => {
    writeAnnouncement(
      rootDir,
      "v1.2.3",
      completeAnnouncement("v1.2.3", "中文发布标题"),
    );

    const result = readReleaseMetadata({ rootDir, tag: "v1.2.3" });

    assert.equal(result.title, "v1.2.3 - 中文发布标题");
    assert.equal(result.relativeBodyPath, "docs/release-notes/v1.2.3-zh.md");
  }));

test("rejects a missing versioned Chinese announcement", () =>
  withFixture((rootDir) => {
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v1.2.3" }),
      /Chinese release announcement is missing/,
    );
  }));

test("rejects duplicate, mismatched, and placeholder release metadata", () =>
  withFixture((rootDir) => {
    writeAnnouncement(
      rootDir,
      "v1.2.3",
      `${completeAnnouncement("v1.2.3", "标题一")}\n<!-- release-title: v1.2.3 - 标题二 -->`,
    );
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v1.2.3" }),
      /exactly one/,
    );

    writeAnnouncement(
      rootDir,
      "v1.2.3",
      completeAnnouncement("v1.2.2", "错误版本"),
    );
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v1.2.3" }),
      /must start with/,
    );

    writeAnnouncement(
      rootDir,
      "v1.2.3",
      completeAnnouncement("v1.2.3", "标题", "TODO"),
    );
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v1.2.3" }),
      /placeholder/,
    );
  }));

test("rejects a release announcement missing required safety content", () =>
  withFixture((rootDir) => {
    const incomplete = completeAnnouncement("v1.2.3", "标题").replace("`auth.json`", "认证文件");
    writeAnnouncement(rootDir, "v1.2.3", incomplete);

    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v1.2.3" }),
      /missing required release or safety content: `auth\.json`/,
    );
  }));

test("rejects unsafe or malformed release tags", () =>
  withFixture((rootDir) => {
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "../v1.2.3" }),
      /must use the form v<semver>/,
    );
    assert.throws(
      () => readReleaseMetadata({ rootDir, tag: "v01.2.3" }),
      /must use the form v<semver>/,
    );
  }));
