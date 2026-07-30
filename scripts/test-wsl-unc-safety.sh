#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dotnet_exe="${DOTNET_EXE:-/mnt/c/Program Files/dotnet/dotnet.exe}"
wsl_test_root="$(mktemp -d /tmp/codex-provider-sync-wsl-unc.XXXXXX)"
sqlite_home="$wsl_test_root/sqlite"

cleanup() {
  rm -rf "$wsl_test_root"
}
trap cleanup EXIT

cd "$repo_dir"
mkdir -p "$sqlite_home"
node --input-type=module - "$sqlite_home/state_5.sqlite" <<'NODE'
import { openDatabase } from "./src/sqlite.js";

const database = await openDatabase(process.argv[2]);
try {
  database.exec(`
    CREATE TABLE threads (
      id TEXT PRIMARY KEY,
      model_provider TEXT,
      cwd TEXT NOT NULL DEFAULT '',
      archived INTEGER NOT NULL DEFAULT 0,
      first_user_message TEXT NOT NULL DEFAULT '',
      model TEXT
    );
    INSERT INTO threads (id, model_provider, cwd, archived, first_user_message)
    VALUES ('thread-wsl-safety', 'apigather', '/tmp', 0, 'hello');
  `);
} finally {
  database.close();
}
NODE

sqlite_home_windows="$(wslpath -w "$sqlite_home")"
project_windows="$(wslpath -w "$repo_dir/desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj")"

CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME="$sqlite_home_windows" \
  "$dotnet_exe" test "$project_windows" --no-restore \
  --filter "Category=WindowsWslIntegration"
