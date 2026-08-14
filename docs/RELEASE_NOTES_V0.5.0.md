# v0.5.0 Technical Release Notes

v0.5.0 hardens the localhost Web UI shipped through PR #80. The browser now pairs through a high-entropy, short-lived, single-use URL fragment and receives a persistent device credential. The service persists only credential hashes and resolves all operational storage paths from server-managed profiles.

The HTTP listener remains restricted to `127.0.0.1`. Browser writes require a paired device credential and an Origin matching the request's actual loopback Host and port, including `localhost`, custom ports, and SSH-forwarded ports. The service is not intended for LAN or public exposure.

History search now waits 300ms after typing, searches immediately on Enter, aborts superseded list/detail requests, and allows only the latest response to update the view. No SQLite or full-text History index is introduced.

The supported runtime floor is Node.js 16.20.2. CI covers 16.20.2 and the current LTS, including dependency installation, Web build, and the full Node test suite. npm and all shipped .NET projects use version 0.5.0.

Validation covers anonymous-page isolation, device authorization, one-time pairing, credential persistence/reset, dynamic loopback Origin checks, loopback-only binding, server-managed profiles, request supersession, Web build, Node tests, and release-version consistency.

## Contributors

Thanks to [@tangquanwei](https://github.com/tangquanwei) for proposing and implementing the Local Web UI, contributing history browsing and the multilingual documentation foundation, and bringing it into v0.5.0 through [PR #80](https://github.com/Dailin521/codex-provider-sync/pull/80). See [PR #73](https://github.com/Dailin521/codex-provider-sync/pull/73) for the original proposal.
