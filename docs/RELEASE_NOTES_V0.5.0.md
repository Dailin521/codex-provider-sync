# v0.5.0 Technical Release Notes

v0.5.0 hardens the localhost Web UI introduced in PR #73. The browser now pairs through a high-entropy, short-lived, single-use URL fragment and receives a persistent device credential. The service persists only credential hashes and resolves all operational storage paths from server-managed profiles.

The HTTP listener remains restricted to `127.0.0.1`. Browser writes require a paired device credential and an Origin matching the request's actual loopback Host and port, including `localhost`, custom ports, and SSH-forwarded ports. The service is not intended for LAN or public exposure.

History search now waits 300ms after typing, searches immediately on Enter, aborts superseded list/detail requests, and allows only the latest response to update the view. No SQLite or full-text History index is introduced.

The supported runtime floor is Node.js 16.20.2. CI covers 16.20.2 and the current LTS, including dependency installation, Web build, and the full Node test suite. npm and all shipped .NET projects use version 0.5.0.

Validation covers anonymous-page isolation, device authorization, one-time pairing, credential persistence/reset, dynamic loopback Origin checks, loopback-only binding, server-managed profiles, request supersession, Web build, Node tests, and release-version consistency.
