#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SEMVER_PATTERN =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/;

const VERSION_PROPERTIES = ["Version", "AssemblyVersion", "FileVersion"];

function parseSemanticVersion(value, source) {
  const match = SEMVER_PATTERN.exec(value);
  if (!match) {
    throw new Error(`${source} must be a valid semantic version; received ${JSON.stringify(value)}.`);
  }

  return {
    value,
    numericVersion: `${match[1]}.${match[2]}.${match[3]}`,
  };
}

function parseReleaseTag(tag) {
  if (typeof tag !== "string" || !tag.startsWith("v")) {
    throw new Error(`Release tag must use the form v<semver>; received ${JSON.stringify(tag)}.`);
  }

  return parseSemanticVersion(tag.slice(1), "Release tag");
}

function collectProjectFiles(directory) {
  const projects = [];

  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      projects.push(...collectProjectFiles(entryPath));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".csproj")) {
      projects.push(entryPath);
    }
  }

  return projects;
}

function isTestProject(relativeProjectPath, projectXml) {
  const normalizedPath = relativeProjectPath.replaceAll("\\", "/");
  return (
    /(?:^|[./_-])tests?(?:[./_-]|$)/i.test(normalizedPath) ||
    /<IsTestProject\b[^>]*>\s*true\s*<\/IsTestProject>/i.test(projectXml) ||
    /<PackageReference\b[^>]*\bInclude\s*=\s*["']Microsoft\.NET\.Test\.Sdk["']/i.test(projectXml)
  );
}

function readPropertyValues(projectXml, propertyName) {
  const xmlWithoutComments = projectXml.replace(/<!--[\s\S]*?-->/g, "");
  const expression = new RegExp(
    `<${propertyName}\\b[^>]*>([\\s\\S]*?)<\\/${propertyName}\\s*>`,
    "gi",
  );

  return [...xmlWithoutComments.matchAll(expression)].map((match) => match[1].trim());
}

function displayPath(rootDir, targetPath) {
  return path.relative(rootDir, targetPath).replaceAll("\\", "/");
}

export function verifyReleaseVersion({ rootDir, tag }) {
  const release = parseReleaseTag(tag);
  const errors = [];

  const packageJsonPath = path.join(rootDir, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
  const packageVersion = packageJson.version;

  try {
    parseSemanticVersion(packageVersion, "package.json version");
  } catch (error) {
    errors.push(error.message);
  }

  if (packageVersion !== release.value) {
    errors.push(
      `package.json version is ${JSON.stringify(packageVersion)}; expected ${JSON.stringify(release.value)} from tag ${tag}.`,
    );
  }

  const packageLockPath = path.join(rootDir, "package-lock.json");
  const packageLock = JSON.parse(fs.readFileSync(packageLockPath, "utf8"));
  const packageLockVersions = [
    ["package-lock.json version", packageLock.version],
    ['package-lock.json packages[""].version', packageLock.packages?.[""]?.version],
  ];

  for (const [source, value] of packageLockVersions) {
    if (value !== release.value) {
      errors.push(
        `${source} is ${JSON.stringify(value)}; expected ${JSON.stringify(release.value)} from tag ${tag}.`,
      );
    }
  }

  const desktopDirectory = path.join(rootDir, "desktop");
  const shippedProjects = collectProjectFiles(desktopDirectory)
    .map((projectPath) => ({
      projectPath,
      projectXml: fs.readFileSync(projectPath, "utf8"),
    }))
    .filter(
      ({ projectPath, projectXml }) =>
        !isTestProject(path.relative(desktopDirectory, projectPath), projectXml),
    )
    .sort((left, right) => left.projectPath.localeCompare(right.projectPath));

  if (shippedProjects.length === 0) {
    errors.push("No shipped .csproj files were found under desktop/.");
  }

  const expectedValues = {
    Version: release.value,
    AssemblyVersion: `${release.numericVersion}.0`,
    FileVersion: `${release.numericVersion}.0`,
  };

  for (const { projectPath, projectXml } of shippedProjects) {
    const projectDisplayPath = displayPath(rootDir, projectPath);

    for (const propertyName of VERSION_PROPERTIES) {
      const values = readPropertyValues(projectXml, propertyName);
      const expectedValue = expectedValues[propertyName];

      if (values.length === 0) {
        errors.push(`${projectDisplayPath} does not declare <${propertyName}>${expectedValue}</${propertyName}>.`);
        continue;
      }

      for (const value of values) {
        if (value !== expectedValue) {
          errors.push(
            `${projectDisplayPath} declares <${propertyName}>${value}</${propertyName}>; expected ${expectedValue}.`,
          );
        }
      }
    }
  }

  if (errors.length > 0) {
    throw new Error(`Release version consistency check failed:\n- ${errors.join("\n- ")}`);
  }

  return {
    tag,
    version: release.value,
    projects: shippedProjects.map(({ projectPath }) => displayPath(rootDir, projectPath)),
  };
}

function parseArguments(argumentsList) {
  if (argumentsList.length !== 2 || argumentsList[0] !== "--tag" || !argumentsList[1]) {
    throw new Error("Usage: node scripts/verify-release-version.js --tag v<semver>");
  }

  return { tag: argumentsList[1] };
}

function main() {
  try {
    const { tag } = parseArguments(process.argv.slice(2));
    const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
    const result = verifyReleaseVersion({
      rootDir: path.resolve(scriptDirectory, ".."),
      tag,
    });
    console.log(
      `Release version check passed: ${result.tag} matches the npm manifests and ${result.projects.length} shipped .csproj files.`,
    );
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
