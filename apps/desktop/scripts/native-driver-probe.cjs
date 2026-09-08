"use strict";

const { app } = require("electron");

async function run() {
  const packagePath = process.env.CPS_NATIVE_DRIVER_PACKAGE;
  const nativeBinding = process.env.CPS_NATIVE_DRIVER_BINDING;
  if (!packagePath || !nativeBinding) throw new Error("Native driver probe inputs are missing.");

  const Database = require(packagePath);
  const db = new Database(":memory:", { nativeBinding });
  try {
    db.exec("CREATE TABLE probe (value INTEGER NOT NULL); INSERT INTO probe VALUES (42);");
    const row = db.prepare("SELECT value FROM probe").get();
    if (row?.value !== 42) throw new Error("Native SQLite probe returned an unexpected row.");
    process.stdout.write(`CPS_NATIVE_DRIVER_RESULT=${JSON.stringify({
      driver: "better-sqlite3",
      electron: process.versions.electron,
      modules: process.versions.modules,
      sqlite: process.versions.sqlite
    })}\n`);
  } finally {
    db.close();
  }
}

app.whenReady()
  .then(run)
  .then(() => app.exit(0))
  .catch((error) => {
    process.stderr.write(`Native driver probe failed: ${error instanceof Error ? error.message : String(error)}\n`);
    app.exit(1);
  });
