let databaseFactoryPromise = null;

function normalizeImportDefault(moduleNamespace) {
  return moduleNamespace.default ?? moduleNamespace;
}

class BetterSqliteDatabase {
  constructor(Database, dbPath, options = {}) {
    this.db = new Database(dbPath, {
      readonly: Boolean(options.readOnly)
    });
  }

  prepare(sql) {
    return this.db.prepare(sql);
  }

  exec(sql) {
    return this.db.exec(sql);
  }

  close() {
    return this.db.close();
  }
}

async function loadDatabaseFactory() {
  try {
    const sqlite = await import("node:sqlite");
    if (sqlite.DatabaseSync) {
      return (dbPath, options) => new sqlite.DatabaseSync(dbPath, options);
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
