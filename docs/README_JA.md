# codex-provider-sync 日本語ガイド

`codex-provider-sync` は、Provider の切り替え後に Codex の過去セッションを再び表示できるようにする、ローカル実行のメタデータ整合性ツールです。セッション本体が削除されたのではなく、rollout、SQLite のスレッド索引、プロジェクト metadata が以前の Provider を参照している場合に使用します。

現在の推奨インターフェースは localhost Web UI です。CLI は自動化と WSL 環境向けに利用できます。Desktop GUI は非推奨で、互換性のための旧版資料のみ残しています。

## クイックスタート：Web UI

Node.js 16 以降が必要です。

```bash
npm install
npm run web:build
npm run web:start
```

またはグローバルにインストールします。

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider web
```

既定のアドレス：`http://127.0.0.1:8791`

```bash
codex-provider web --no-open
codex-provider web --port 8792
```

サービスは `127.0.0.1` のみに bind します。プロセスごとにランダムな API セッショントークンを発行し、Origin を検証します。sync、switch、restore、prune などの書き込み操作は同時に一つだけ実行されます。

## Web UI の機能

- Overview：現在の Provider、rollout/SQLite 分布、整合性、プロジェクト可視性を確認。
- Chat History：rollout JSONL を読み取り専用で解析し、ユーザーと agent のメッセージを表示。
- タイトル、プロジェクト、Provider、メッセージ本文の検索と、active/archived フィルター。
- 1 ページ 50 セッションのサーバーサイドページング。
- 詳細画面では最新 200 件の読み取り可能なメッセージを表示し、安全な限定 Markdown とコードブロックをサポート。
- Sync / Switch：config を変更しない同期、Provider 切り替え後の同期、model 方針の選択。
- Backups：metadata v2 バックアップの確認、内容別の復元、古いバックアップの削除。

Chat History は raw JSONL、token、ツール呼び出し引数、`encrypted_content` をブラウザーへ返しません。

## CLI：自動化と WSL

CLI と Web UI は同じ `src/service.js` コアロジックを使用します。

```bash
codex-provider status
codex-provider sync
codex-provider switch apigather
codex-provider switch apigather --model "MiniMax-M3"
codex-provider restore <backup-dir>
codex-provider prune-backups --keep 5
codex-provider watch
```

すべての主要コマンドで `--codex-home <PATH>` と `--sqlite-home <PATH>` を指定できます。Windows の Codex Home と WSL の SQLite を組み合わせる場合は、WSL 内で Linux パスを使って CLI を実行してください。

```bash
codex-provider sync \
  --codex-home /mnt/c/Users/you/.codex \
  --sqlite-home /home/you/.codex/sqlite
```

## SQLite Home の解決

優先順位は CLI/GUI override → `config.toml` の root `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite` です。明示的に指定された SQLite Home が存在しない場合、別のデータベースへフォールバックしません。旧 `<Codex Home>/state_5.sqlite` を確認できるのは既定レイアウトだけです。

## 安全性と制限

- sync / switch の前に `~/.codex/backups_state/provider-sync/<timestamp>` へバックアップを作成します。
- メッセージ本文、タイトル、認証情報、`auth.json`、`updated_at` は変更しません。
- SQLite がロックされている場合は Codex CLI、Codex App、app-server を終了してから再実行してください。
- ロック中の rollout はスキップされ、セッション終了後に再同期できます。
- `encrypted_content` を含む履歴は一覧に表示できても、Provider/account をまたぐと再開や compact に失敗する場合があります。

## Desktop GUI の状態

Desktop GUI は非推奨です。既存の Windows/macOS 版は互換性のために残っていますが、新機能は Web UI を優先します。

- [Web UI 使用説明（中国語）](README_WEB_UI_ZH.md)
- [English README](../README.md)
- [旧 Windows GUI 説明](README_GUI_ZH.md)

## 開発とテスト

```bash
npm install
npm run web:build
npm test
git diff --check
```

## License

MIT
