import path from "node:path";

let databaseFactoryPromise = null;

function nativeSqlitePath(value) {
  if (process.platform !== "win32"
      || typeof value !== "string"
      || value === ":memory:"
      || value.startsWith("file:")) {
    return value;
  }
  // SQLite's Windows VFS needs the extended-length form once deeply nested
  // managed backup/snapshot paths cross MAX_PATH.
  return path.toNamespacedPath(path.resolve(value));
}

function normalizeImportDefault(moduleNamespace) {
  return moduleNamespace.default ?? moduleNamespace;
}

class BetterSqliteDatabase {
  constructor(Database, dbPath, options = {}) {
    this.driver = "better-sqlite3";
    this.db = new Database(nativeSqlitePath(dbPath), {
      readonly: Boolean(options.readOnly)
    });
  }

  prepare(sql) {
    return this.db.prepare(sql);
  }

  exec(sql) {
    return this.db.exec(sql);
  }

  async backup(destinationPath, options = {}) {
    return this.db.backup(nativeSqlitePath(destinationPath), options);
  }

  close() {
    return this.db.close();
  }
}

class NodeSqliteDatabase {
  constructor(sqlite, dbPath, options = {}) {
    this.driver = "node:sqlite";
    this.sqlite = sqlite;
    this.db = new sqlite.DatabaseSync(nativeSqlitePath(dbPath), options);
  }

  prepare(sql) {
    return this.db.prepare(sql);
  }

  exec(sql) {
    return this.db.exec(sql);
  }

  async backup(destinationPath, options = {}) {
    return this.sqlite.backup(this.db, nativeSqlitePath(destinationPath), options);
  }

  close() {
    return this.db.close();
  }
}

async function loadDatabaseFactory() {
  try {
    const sqlite = await import("node:sqlite");
    if (sqlite.DatabaseSync && typeof sqlite.backup === "function") {
      return (dbPath, options) => new NodeSqliteDatabase(sqlite, dbPath, options);
    }
  } catch {
    // Older Node.js releases do not include node:sqlite.
  }

  try {
    const betterSqlite3 = normalizeImportDefault(await import("better-sqlite3"));
    return (dbPath, options) => new BetterSqliteDatabase(betterSqlite3, dbPath, options);
  } catch (error) {
    throw new Error(
      "SQLite support requires Node.js with node:sqlite, or the optional better-sqlite3 dependency on older Node.js. "
        + "For local/link installs, run npm install --include=optional in the package directory. "
        + "For normal installs, reinstall without --omit=optional. "
        + `Original error: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

async function getDatabaseFactory() {
  databaseFactoryPromise ??= loadDatabaseFactory();
  return databaseFactoryPromise;
}

export async function openDatabase(dbPath, options = {}) {
  const createDatabase = await getDatabaseFactory();
  return createDatabase(dbPath, options);
}
