# Changelog

All notable changes to this project will be documented here. This project
follows [Semantic Versioning](https://semver.org/).

## 0.1.0 — 2026-07-24

### Added

- Synchronous local reads and writes with a familiar JavaScript `Map` interface.
- A 50ms write-behind buffer that coalesces repeated mutations by key.
- Explicit `flush()` durability boundaries and serialized batch persistence.
- Global and key-specific change subscriptions with source metadata.
- Atomic Redis Hash patches and Redis Stream events.
- Ten-second time-based Stream retention using `XADD MINID`.
- Cross-process synchronization for set, delete, and clear operations.
- Periodic snapshot reloads and automatic recovery after trimmed or malformed events.
- Custom serialization and error reporting hooks.
- Optional shallow MobX adapter with action-batched remote patches.
- Dual ESM/CommonJS package output with TypeScript declarations.
