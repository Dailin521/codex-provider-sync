# codex-provider-sync 한국어 안내

`codex-provider-sync`는 Provider를 변경한 뒤 Codex의 이전 세션을 다시 표시할 수 있도록 돕는 로컬 메타데이터 정합성 도구입니다. 세션 파일이 삭제된 것이 아니라 rollout, SQLite 스레드 인덱스 또는 프로젝트 metadata가 이전 Provider를 가리킬 때 사용합니다.

현재 권장 인터페이스는 localhost Web UI입니다. CLI는 자동화와 WSL 환경에서 사용합니다. Desktop GUI는 더 이상 권장하지 않으며 호환성을 위한 이전 문서만 유지합니다.

## 빠른 시작: Web UI

Node.js 16.20.2 이상이 필요합니다.

```bash
npm install
npm run web:build
npm run web:start
```

또는 전역 설치:

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider web
```

기본 주소: `http://127.0.0.1:8791`

```bash
codex-provider web --no-open
codex-provider web --port 8792
```

서비스는 `127.0.0.1`에만 바인딩됩니다. 프로세스마다 임의의 API 세션 토큰을 만들고 Origin을 검증합니다. sync, switch, restore, prune 같은 쓰기 작업은 동시에 하나만 실행됩니다.

## Web UI 기능

- Overview: 현재 Provider, rollout/SQLite 분포, 정합성, 프로젝트 가시성 진단.
- Chat History: rollout JSONL을 읽기 전용으로 분석해 사용자와 agent 메시지를 표시.
- 제목, 프로젝트 경로, Provider, 메시지 본문 검색 및 active/archived 필터.
- 페이지당 기본 50개 세션의 서버 측 페이지네이션.
- 상세 화면에서 최근 200개의 읽을 수 있는 메시지를 표시하고 제한된 안전 Markdown과 코드 블록을 지원.
- Sync / Switch: 설정을 바꾸지 않는 동기화, Provider 전환 후 동기화, model 정책 선택.
- Backups: metadata v2 백업 조회, 내용별 복원, 오래된 백업 정리.

Chat History는 raw JSONL, token, 도구 호출 인자, `encrypted_content`를 브라우저에 반환하지 않습니다.

## CLI: 자동화 및 WSL

CLI와 Web UI는 동일한 `src/service.js` 핵심 로직을 사용합니다.

```bash
codex-provider status
codex-provider sync
codex-provider switch apigather
codex-provider switch apigather --model "MiniMax-M3"
codex-provider restore <backup-dir>
codex-provider prune-backups --keep 5
codex-provider watch
```

주요 명령은 `--codex-home <PATH>` 및 `--sqlite-home <PATH>`를 지원합니다. Windows Codex Home과 WSL SQLite를 함께 사용할 때는 WSL 안에서 Linux 경로로 CLI를 실행하세요.

```bash
codex-provider sync \
  --codex-home /mnt/c/Users/you/.codex \
  --sqlite-home /home/you/.codex/sqlite
```

## SQLite Home 해석

우선순위는 CLI/GUI override → `config.toml`의 root `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`입니다. 명시적으로 지정한 SQLite Home이 없으면 다른 데이터베이스로 조용히 fallback하지 않습니다. 기존 `<Codex Home>/state_5.sqlite`를 확인하는 것은 기본 레이아웃에서만 허용됩니다.

## 안전성과 제한

- sync / switch 전에 `~/.codex/backups_state/provider-sync/<timestamp>`에 백업을 생성합니다.
- 메시지 본문, 제목, 인증 정보, `auth.json`, `updated_at`은 변경하지 않습니다.
- SQLite가 잠겨 있으면 Codex CLI, Codex App, app-server를 종료한 뒤 다시 실행하세요.
- 잠긴 rollout 파일은 건너뛰며 활성 세션이 끝난 후 다시 동기화할 수 있습니다.
- `encrypted_content`가 포함된 기록은 목록에는 보일 수 있지만 Provider/account를 넘나들면 재개나 compact가 실패할 수 있습니다.

## Desktop GUI 상태

Desktop GUI는 더 이상 권장하지 않습니다. 기존 Windows/macOS 버전은 호환성을 위해 남아 있지만 새 기능은 Web UI를 우선합니다.

- [Web UI 사용 설명(중국어)](README_WEB_UI_ZH.md)
- [English README](../README.md)
- [기존 Windows GUI 설명](README_GUI_ZH.md)

## 개발 및 테스트

```bash
npm install
npm run web:build
npm test
git diff --check
```

## License

MIT
