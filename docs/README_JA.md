<div align="center">

# codex-provider-sync

### Provider 切り替え後も Codex の過去セッションを再表示する

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![CLI / Web](https://img.shields.io/npm/v/%40dailin521%2Fcodex-provider-sync?label=CLI%20%2F%20Web)](https://www.npmjs.com/package/@dailin521/codex-provider-sync)
[![Windows GUI](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync?label=Windows%20GUI)](https://github.com/Dailin521/codex-provider-sync/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)
[![Community](https://img.shields.io/badge/community-LINUX%20DO-2ea043.svg)](https://linux.do/)

[**Windows GUI をダウンロード**](https://github.com/Dailin521/codex-provider-sync/releases/latest) · [ローカル Web UI を使う（CLI が必要）](#ローカル-web-ui)

言語：[中文](../README.md) · [English](README_EN.md) · **日本語** · [한국어](README_KO.md)

</div>

## 解決すること

`model_provider` を切り替えた後、既存セッションが Codex Desktop や `/resume` から消えることがあります。**データ自体は通常ディスク上に残っています**。セッションファイルと SQLite インデックス内の Provider 情報だけが同期されていません。

このツールはセッションファイルと SQLite インデックスを同期してセッションの可視性を復元し、書き込み前にバックアップを作成します。ログイン、アカウント切り替え、`auth.json`、メッセージ本文は扱いません。

> **同期が必要なのはいつですか？**
>
> - **通常のケース：**公式 OpenAI とカスタム中継先を切り替える場合。公式 OpenAI は常に `openai` を使用するため Provider ID が変わり、履歴の同期が必要です。
> - **既存履歴で ID が混在している場合：**旧セッションに異なる Provider ID が記録されているため、現在の Provider に揃える必要があります。
> - **同期が不要なケース：**同じ Provider ID を共有するカスタム中継先だけを切り替える場合、または CCSwitch などがすでに履歴を同期している場合です。

## クイックスタート

> Windows GUI とローカル Web UI の画面表示は現在、簡体字中国語のみです。

| 利用場面 | 推奨する入口 |
| --- | --- |
| Windows デスクトップ | [Windows GUI をダウンロード](https://github.com/Dailin521/codex-provider-sync/releases/latest)・[使い方](#windows-gui) |
| macOS デスクトップ | [ローカル Web UI（CLI が必要）](#ローカル-web-ui)・[ネイティブ GUI のビルド手順（英語）](README_MAC_GUI_EN.md) |
| ブラウザ UI またはクロスプラットフォーム利用 | [ローカル Web UI（CLI が必要）](#ローカル-web-ui) |
| スクリプト、CI、または WSL | [CLI](#cli) |

### Windows GUI

[Releases](https://github.com/Dailin521/codex-provider-sync/releases/latest) から `CodexProviderSync.exe` をダウンロードします。

1. 「刷新」（Refresh）をクリックします。
2. 対象の Provider を選択します。
3. 「立即同步」（Sync Now）をクリックします。

コード署名は付与していないため、Windows でセキュリティ警告が表示される場合があります。本プロジェクトの Releases からのみダウンロードしてください。

[Windows GUI の詳細（中国語）](README_GUI_ZH.md)

### ローカル Web UI

ローカル Web UI は CLI に含まれています。Node.js `16.20.2+` をインストールし、本プロジェクトの公式 npm パッケージをインストールして起動します。

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider web
```

![Web UI 概要](../images/README/2026-08-05T03-53-48.708Z.png)

よく使うオプション:

```bash
codex-provider web --no-open       # ブラウザを自動で開かない
codex-provider web --port 8792     # ポートを指定する
codex-provider web --reset-access  # ブラウザを再ペアリングする
```

Web UI はデフォルトで `127.0.0.1` のみで待ち受け、ブラウザを自動で開いてペアリングします。保存先はページ上部の保存設定（Profile）で管理します。書き込み操作には確認が必要です。サービスは `Ctrl+C` で停止します。

#### Provider 切り替え後に履歴を同期する

1. CCSwitch など普段使用しているツールで Provider を切り替えます。
2. 必要に応じて Web UI で「读取状态」（Read Status）をクリックします（省略可）。
3. 「仅同步元数据」（Metadata Only）のまま対象 Provider を選択し、同期の実行を確認します。
4. 「Provider 元数据已对齐」（Provider Metadata Aligned）が表示されれば完了です。

> **注意：** メタデータ同期で復元されるのは履歴の可視性だけです。Provider をまたいで旧セッションを続行すると、切り替え先のバックエンドが `encrypted_content` の推論内容を復号できず、続行や compact に失敗する場合があります。

[Web UI の詳細（中国語）](README_WEB_UI_ZH.md)

### CLI

CLI は Node.js `16.20.2+` をサポートします。Node.js のインストール後、本プロジェクトの公式 npm パッケージをインストールします。

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider status
codex-provider sync
```

| コマンド | 用途 |
| --- | --- |
| `codex-provider status` | Provider、rollout、SQLite の状態を確認する |
| `codex-provider sync` | 現在の Provider に同期する |
| `codex-provider switch <provider-id>` | Provider を切り替えてから同期する |
| `codex-provider restore <backup-dir>` | バックアップを復元する |
| `codex-provider watch` | 設定と SQLite の変更を監視する |

`switch` は、対象 Provider section に `model` が定義されている場合、デフォルトでルートレベルの `model` も更新します。現在の値を保持するには `--keep-root-model`、明示的に指定するには `--model <name>` を使用します。

SQLite Home の解決順序: `--sqlite-home` → `config.toml` ルートの `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。デフォルトレイアウトだけが `<Codex Home>/state_5.sqlite` にフォールバックします。

## 現在のアーキテクチャ

```mermaid
flowchart LR
    Browser["Browser Web UI"] --> WebServer["Local Node Web Server<br/>127.0.0.1"]
    WebServer --> NodeService["Node Service"]
    CLI["Node CLI"] --> NodeService

    WindowsGUI["Windows GUI"] --> Application[".NET Application"]
    Application --> DotNetCore[".NET Core"]
    MacGUI["macOS GUI"] --> DotNetCore

    NodeService --> Storage["Codex Storage"]
    DotNetCore --> Storage

    Storage --> Config["config.toml"]
    Storage --> Rollouts["sessions / archived_sessions"]
    Storage --> SQLite["state_5.sqlite"]
    Storage --> Backups["managed backups"]
```

- Web UI と CLI は同じ Node サービスロジックを使用します。
- Windows GUI は Application 層を通じて .NET Core を呼び出し、macOS GUI は現在 .NET Core を直接呼び出します。
- Node サービスと .NET Core は同じ設定、rollout、SQLite、バックアップの安全境界を扱います。

## 安全上の境界

- `sync` / `switch` の前に、毎回 `<Codex Home>/backups_state/provider-sync/<timestamp>` へバックアップします。デフォルトの Codex Home では `~/.codex/backups_state/provider-sync/<timestamp>` です。
- メッセージ本文、セッションタイトル、認証情報、`auth.json`、`updated_at` は変更しません。
- SQLite が使用中の場合は、Codex、Codex App、app-server を閉じてから再試行してください。
- アクティブなセッションが rollout をロックしている場合、他のファイルは続行します。セッション終了後にもう一度同期してください。
- Provider またはアカウントをまたいで旧セッションを続行すると、切り替え先のバックエンドが `encrypted_content` を復号できず、続行や compact に失敗する場合があります。その場合は元の Provider／アカウントに戻すか、新しいセッションを開始してください。
- Windows から WSL UNC SQLite Home に直接書き込むことはできません。WSL に入り、Linux パスで CLI を実行してください。

## ドキュメント

- [AI / Agent ガイド](../AGENTS.md)

- [Windows GUI（中国語）](README_GUI_ZH.md)
- [Web UI（中国語）](README_WEB_UI_ZH.md)
- [中文](../README.md) · [English](README_EN.md) · 日本語 · [한국어](README_KO.md)
- [macOS GUI: 中文](README_MAC_GUI_ZH.md) · [English](README_MAC_GUI_EN.md)
- [仕組み（中国語）](WORKING_PRINCIPLE_ZH.md) · [変更履歴](../CHANGELOG.md) · [コントリビューションガイド](../CONTRIBUTING.md)

## 開発

```bash
npm ci
npm run web:build
npm run web:start
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
```

メンテナーは、Windows GUI の Release とは独立して CLI/Web パッケージを公開できます。[npm 公開ガイド（中国語）](NPM_PUBLISHING.md)を参照してください。

## 謝辞

ローカル Web UI を提案・実装し、履歴閲覧機能と多言語ドキュメントの基盤を提供するとともに、[PR #80](https://github.com/Dailin521/codex-provider-sync/pull/80) を通じて v0.5.0 に導入した [@tangquanwei](https://github.com/tangquanwei) に感謝します。最初の提案は [PR #73](https://github.com/Dailin521/codex-provider-sync/pull/73) を参照してください。

## License

MIT
