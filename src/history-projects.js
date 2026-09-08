import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const MAX_GLOBAL_STATE_BYTES = 1024 * 1024;

function text(value, max = 1024) {
  return typeof value === "string" && value.length <= max && !/[\x00-\x1f\x7f]/.test(value) ? value.trim() : "";
}

function pathInfo(value) {
  let source = text(value, 32768);
  if (!source) return null;
  const unc = source.match(/^\\\\\?\\UNC\\(.+)$/i);
  const drive = source.match(/^\\\\\?\\([A-Za-z]:)(?:[\\/](.*))?$/);
  if (unc) source = `\\\\${unc[1]}`;
  else if (drive) source = drive[2] ? `${drive[1]}\\${drive[2]}` : `${drive[1]}\\`;
  const windows = /^[A-Za-z]:[\\/]/.test(source) || /^\\\\/.test(source);
  const paths = windows ? path.win32 : path.posix;
  if (!paths.isAbsolute(source)) return null;
  const normalized = paths.normalize(source);
  const root = paths.parse(normalized).root;
  const directory = normalized.length > root.length ? normalized.replace(/[\\/]+$/, "") : root;
  const key = `${windows ? "win" : "posix"}:${windows ? directory.toLowerCase() : directory}`;
  return { key, value: directory, windows, name: (paths.basename(directory) || root.replace(/[\\/]/g, "") || "/").slice(0, 160) };
}

function projectId(key) { return crypto.createHash("sha256").update(key, "utf8").digest("hex"); }

function isWithin(root, candidate) {
  if (root.windows !== candidate.windows) return false;
  const paths = root.windows ? path.win32 : path.posix;
  const relative = paths.relative(root.value, candidate.value);
  return relative === "" || (!paths.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${paths.sep}`));
}

function array(value) { return Array.isArray(value) ? value.filter((item) => text(item, 32768)) : (text(value, 32768) ? [value] : []); }

function sameOpenedFile(left, right) {
  return String(left.dev) === String(right.dev) && String(left.ino) === String(right.ino)
    && Number(left.size) === Number(right.size) && Number(left.mtimeMs) === Number(right.mtimeMs)
    && Number(left.ctimeMs) === Number(right.ctimeMs);
}

async function readBoundedGlobalState(codexHome) {
  const lexicalHome = path.resolve(codexHome);
  const filePath = path.join(lexicalHome, ".codex-global-state.json");
  let handle;
  try {
    const physicalHome = path.resolve(await fs.realpath(lexicalHome));
    const before = await fs.lstat(filePath);
    if (!before.isFile() || before.isSymbolicLink() || before.size > MAX_GLOBAL_STATE_BYTES) return null;
    const physicalPath = path.resolve(await fs.realpath(filePath));
    if (!isWithin({ windows: process.platform === "win32", value: physicalHome }, { windows: process.platform === "win32", value: physicalPath })) return null;
    handle = await fs.open(filePath, "r");
    const opened = await handle.stat();
    if (!sameOpenedFile(before, opened) || opened.size > MAX_GLOBAL_STATE_BYTES) return null;
    const buffer = Buffer.allocUnsafe(MAX_GLOBAL_STATE_BYTES + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    if (bytesRead > MAX_GLOBAL_STATE_BYTES) return null;
    const after = await fs.lstat(filePath);
    const finalPhysical = path.resolve(await fs.realpath(filePath));
    if (after.isSymbolicLink() || !sameOpenedFile(opened, after) || finalPhysical !== physicalPath) return null;
    return JSON.parse(buffer.subarray(0, bytesRead).toString("utf8"));
  } catch { return null; } finally { await handle?.close().catch(() => {}); }
}

/** Read local display metadata only. This never follows or probes a workspace path. */
export async function readHistoryWorkspaceMetadata(codexHome) {
  const state = await readBoundedGlobalState(codexHome);
  if (!state || typeof state !== "object" || Array.isArray(state)) return [];
  const labels = state["electron-workspace-root-labels"];
  const roots = [...array(state["project-order"]), ...array(state["electron-saved-workspace-roots"]), ...array(state["active-workspace-roots"])];
  const labelByKey = new Map();
  if (labels && typeof labels === "object" && !Array.isArray(labels)) for (const [candidate, label] of Object.entries(labels)) {
    const info = pathInfo(candidate); const safeLabel = text(label, 160);
    if (info && safeLabel && !labelByKey.has(info.key)) labelByKey.set(info.key, safeLabel);
  }
  const seen = new Set();
  return roots.flatMap((value) => {
    const info = pathInfo(value);
    if (!info || seen.has(info.key)) return [];
    seen.add(info.key);
    return [{ ...info, id: projectId(info.key), name: labelByKey.get(info.key) || info.name }];
  });
}

function matches(session, options, projectName) {
  if (options.provider && session.provider !== options.provider) return false;
  if (options.archived !== "all" && session.archived !== (options.archived === "archived")) return false;
  if (options.sessionKind !== "all" && session.sessionKind !== options.sessionKind) return false;
  if (options.projectText && !String(session.cwd || "").toLowerCase().includes(options.projectText)) return false;
  if (!options.query) return true;
  const metadata = [session.title, session.id, session.nativeSessionId, session.cwd, session.provider, session.subagentName, projectName].filter(Boolean).join("\n").toLowerCase();
  return metadata.includes(options.query) || session.messageQueryMatched === true;
}

function page(items, pageNumber, pageSize) {
  const start = (pageNumber - 1) * pageSize;
  return { page: pageNumber, pageSize, total: items.length, hasNextPage: start + pageSize < items.length, sessions: items.slice(start, start + pageSize) };
}

/** Build a display-only project forest from already-read session metadata. */
export async function listHistoryProjects(codexHome, sessions, options) {
  const workspaces = await readHistoryWorkspaceMetadata(codexHome);
  const nodes = new Map(sessions.map((session) => [session.id, { session, parent: null, children: [], orphan: false, root: null, project: null, directMatch: false, visible: false }]));
  for (const node of nodes.values()) {
    const parentId = node.session.parentSessionId;
    if (node.session.sessionKind !== "subagent") continue;
    if (parentId && parentId !== node.session.id && nodes.has(parentId)) node.parent = nodes.get(parentId);
    else node.orphan = true;
  }
  // Functional graph: each component must terminate in a concrete main root.
  for (const start of nodes.values()) {
    if (start.root || start.orphan) continue;
    const trail = []; const positions = new Map(); let current = start;
    while (current && !current.root && !current.orphan && !positions.has(current)) {
      positions.set(current, trail.length); trail.push(current);
      if (current.session.sessionKind === "main" && !current.parent) break;
      current = current.parent;
    }
    const validRoot = current?.root ?? (current && !current.orphan && current.session.sessionKind === "main" && !current.parent ? current : null);
    if (validRoot) { validRoot.root = validRoot; for (const node of trail) node.root = validRoot; }
    else for (const node of trail) node.orphan = true;
  }
  for (const node of nodes.values()) if (node.parent && !node.orphan && !node.parent.orphan) node.parent.children.push(node);
  const directories = new Map();
  const projectForRoot = (session) => {
    const cwd = pathInfo(session.cwd);
    if (!cwd) return { id: "unassigned", name: "Unassigned", kind: "unassigned" };
    let workspace = null;
    for (const candidate of workspaces) if (isWithin(candidate, cwd) && (!workspace || candidate.value.length > workspace.value.length)) workspace = candidate;
    if (workspace) return { id: workspace.id, name: workspace.name, kind: "workspace" };
    if (!directories.has(cwd.key)) directories.set(cwd.key, { id: projectId(cwd.key), name: cwd.name, kind: "directory" });
    return directories.get(cwd.key);
  };
  for (const node of nodes.values()) if (node.orphan) node.project = { id: "orphans", name: "Orphans", kind: "orphans" }; else if (node.root === node) node.project = projectForRoot(node.session);
  for (const node of nodes.values()) if (!node.orphan && node.root !== node) node.project = node.root.project;
  for (const node of nodes.values()) node.directMatch = matches(node.session, options, node.project.kind === "workspace" || node.project.kind === "directory" ? node.project.name : "");
  const stack = [...nodes.values()].filter((node) => !node.orphan && node.root === node).map((node) => [node, false]);
  while (stack.length) {
    const [node, visited] = stack.pop();
    if (!visited) { stack.push([node, true]); for (const child of node.children) stack.push([child, false]); }
    else node.visible = node.directMatch || node.children.some((child) => child.visible);
  }
  for (const node of nodes.values()) if (node.orphan) node.visible = node.directMatch;
  const descriptors = new Map();
  for (const workspace of workspaces) descriptors.set(workspace.id, { id: workspace.id, name: workspace.name, kind: "workspace", total: 0 });
  for (const directory of directories.values()) descriptors.set(directory.id, { ...directory, total: 0 });
  descriptors.set("unassigned", { id: "unassigned", name: "Unassigned", kind: "unassigned", total: 0 });
  for (const node of nodes.values()) if (node.visible && (node.orphan || node.root === node)) {
    const descriptor = descriptors.get(node.project.id) ?? { ...node.project, total: 0 };
    descriptor.total += 1; descriptors.set(node.project.id, descriptor);
  }
  const kindOrder = { workspace: 0, directory: 1, unassigned: 2, orphans: 3 };
  const projects = [...descriptors.values()].filter((entry) => entry.total > 0).sort((a, b) => kindOrder[a.kind] - kindOrder[b.kind]);
  const parent = options.parentId === undefined ? null : nodes.get(options.parentId) ?? null;
  const selected = options.projectId !== undefined
    ? projects.find((entry) => entry.id === options.projectId) ?? null
    : parent && !parent.orphan
      ? projects.find((entry) => entry.id === parent.project.id) ?? null
      : projects.find((entry) => entry.kind !== "orphans") ?? null;
  let items = [];
  if (options.parentId !== undefined) {
    if (parent && selected && !parent.orphan && parent.project.id === selected.id) items = parent.children.filter((node) => node.visible);
  } else if (selected) items = [...nodes.values()].filter((node) => node.visible && node.project.id === selected.id && (node.orphan || node.root === node));
  if (options.sessionKind === "main") items = items.filter((node) => node.session.sessionKind === "main");
  items.sort((a, b) => Date.parse(b.session.updatedAt || 0) - Date.parse(a.session.updatedAt || 0) || b.session.mtimeMs - a.session.mtimeMs);
  const result = page(items, options.page, options.pageSize);
  return { view: "projects", projects, projectId: selected?.id ?? null, ...result, sessions: result.sessions.map((node) => ({ ...node.session, project: node.project, childCount: node.children.filter((child) => child.visible).length })) };
}
