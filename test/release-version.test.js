import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { verifyReleaseVersion } from "../scripts/verify-release-version.js";

function createFixture({ packageVersion = "1.2.3", projects = [] } = {}) {
  const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "release-version-"));

  fs.mkdirSync(path.join(rootDir, "desktop"), { recursive: true });
  fs.writeFileSync(
    path.join(rootDir, "package.json"),
    `${JSON.stringify({ name: "fixture", version: packageVersion }, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(rootDir, "package-lock.json"),
    `${JSON.stringify(
      {
        name: "fixture",
        version: packageVersion,
        lockfileVersion: 3,
        packages: { "": { name: "fixture", version: packageVersion } },
      },
      null,
      2,
    )}\n`,
  );

  for (const project of projects) {
    const projectPath = path.join(rootDir, "desktop", project.path);
    fs.mkdirSync(path.dirname(projectPath), { recursive: true });
    fs.writeFileSync(projectPath, project.xml);
  }

  return rootDir;
}

function withFixture(options, operation) {
  const rootDir = createFixture(options);
  try {
    return operation(rootDir);
  } finally {
    fs.rmSync(rootDir, { force: true, recursive: true });
  }
}

function createProjectXml({
  version = "1.2.3",
  assemblyVersion = "1.2.3.0",
  fileVersion = "1.2.3.0",
} = {}) {
  return `<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <Version>${version}</Version>
    <AssemblyVersion>${assemblyVersion}</AssemblyVersion>
    <FileVersion>${fileVersion}</FileVersion>
  </PropertyGroup>
</Project>
`;
}

test("current repository versions match an explicit tag", () => {
  const testDirectory = path.dirname(fileURLToPath(import.meta.url));
  const rootDir = path.resolve(testDirectory, "..");
  const packageJson = JSON.parse(fs.readFileSync(path.join(rootDir, "package.json"), "utf8"));

  const result = verifyReleaseVersion({ rootDir, tag: `v${packageJson.version}` });

  assert.deepEqual(result.projects, [
    "desktop/CodexProviderSync.App/CodexProviderSync.App.csproj",
    "desktop/CodexProviderSync.Application/CodexProviderSync.Application.csproj",
    "desktop/CodexProviderSync.Automation/CodexProviderSync.Automation.csproj",
    "desktop/CodexProviderSync.Core/CodexProviderSync.Core.csproj",
    "desktop/CodexProviderSync.GuiE2E/CodexProviderSync.GuiE2E.csproj",
    "desktop/CodexProviderSync.Mac/CodexProviderSync.Mac.csproj",
  ]);
});

test("validates every discovered shipped project and ignores test projects", () =>
  withFixture(
    {
      projects: [
        { path: "Product/Product.csproj", xml: createProjectXml() },
        { path: "NewGui/NewGui.csproj", xml: createProjectXml() },
        {
          path: "Product.Tests/Product.Tests.csproj",
          xml: '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup /></Project>',
        },
      ],
    },
    (rootDir) => {
      const result = verifyReleaseVersion({ rootDir, tag: "v1.2.3" });

      assert.deepEqual(result.projects, [
        "desktop/NewGui/NewGui.csproj",
        "desktop/Product/Product.csproj",
      ]);
    },
  ));

test("reports tag, package, and project version drift together", () =>
  withFixture(
    {
      packageVersion: "1.2.2",
      projects: [
        {
          path: "Product/Product.csproj",
          xml: createProjectXml({ version: "1.2.1", fileVersion: "1.2.1.0" }),
        },
      ],
    },
    (rootDir) => {
      assert.throws(
        () => verifyReleaseVersion({ rootDir, tag: "v1.2.3" }),
        (error) => {
          assert.match(error.message, /package\.json version is "1\.2\.2"/);
          assert.match(error.message, /<Version>1\.2\.1<\/Version>; expected 1\.2\.3/);
          assert.match(
            error.message,
            /<FileVersion>1\.2\.1\.0<\/FileVersion>; expected 1\.2\.3\.0/,
          );
          return true;
        },
      );
    },
  ));

test("rejects stale package-lock version metadata", () =>
  withFixture(
    {
      projects: [{ path: "Product/Product.csproj", xml: createProjectXml() }],
    },
    (rootDir) => {
      fs.writeFileSync(
        path.join(rootDir, "package-lock.json"),
        `${JSON.stringify(
          {
            name: "fixture",
            version: "1.2.2",
            lockfileVersion: 3,
            packages: { "": { name: "fixture", version: "1.2.1" } },
          },
          null,
          2,
        )}\n`,
      );

      assert.throws(
        () => verifyReleaseVersion({ rootDir, tag: "v1.2.3" }),
        (error) => {
          assert.match(error.message, /package-lock\.json version is "1\.2\.2"/);
          assert.match(error.message, /packages\[""\]\.version is "1\.2\.1"/);
          return true;
        },
      );
    },
  ));

test("fails when a shipped project omits a release version declaration", () =>
  withFixture(
    {
      projects: [
        {
          path: "Product/Product.csproj",
          xml: `<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <Version>1.2.3</Version>
    <AssemblyVersion>1.2.3.0</AssemblyVersion>
  </PropertyGroup>
</Project>`,
        },
      ],
    },
    (rootDir) => {
      assert.throws(
        () => verifyReleaseVersion({ rootDir, tag: "v1.2.3" }),
        /does not declare <FileVersion>1\.2\.3\.0<\/FileVersion>/,
      );
    },
  ));

test("supports prerelease tags while keeping numeric assembly versions", () =>
  withFixture(
    {
      packageVersion: "1.2.3-rc.1",
      projects: [
        {
          path: "Product/Product.csproj",
          xml: createProjectXml({ version: "1.2.3-rc.1" }),
        },
      ],
    },
    (rootDir) => {
      assert.doesNotThrow(() => verifyReleaseVersion({ rootDir, tag: "v1.2.3-rc.1" }));
    },
  ));

test("requires a strict v-prefixed semantic version tag", () =>
  withFixture(
    {
      projects: [{ path: "Product/Product.csproj", xml: createProjectXml() }],
    },
    (rootDir) => {
      assert.throws(
        () => verifyReleaseVersion({ rootDir, tag: "1.2.3" }),
        /Release tag must use the form v<semver>/,
      );
      assert.throws(
        () => verifyReleaseVersion({ rootDir, tag: "v01.2.3" }),
        /Release tag must be a valid semantic version/,
      );
    },
  ));
