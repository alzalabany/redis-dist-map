# Changelog

All notable changes to this project will be documented here. This project
follows [Semantic Versioning](https://semver.org/).

## 0.4.1 — 2026-09-15

### Changed

- `set()` now skips values that serialize identically to the cached value,
  avoiding redundant change events, Redis writes, revisions, and Stream entries.

## 0.4.0 — 2026-09-15

### Changed

- Replaced periodic Hash snapshot polling with atomic map revisions. Readers
  now reload the authoritative snapshot only after a Stream revision gap or a
  malformed event.
- Removed the `synchronizeIntervalMs` option.

### Removed

- The optional MobX adapter and `redis-dist-map/mobx` subpath export. Use
  `onChange()` subscriptions to observe local and remote map changes.

## 0.3.0 — 2026-07-24

### Added

- `createSharedCounter()` for direct, atomic Redis counters that are never
  buffered or cached.
- Optional per-key `ttlMs` windows set atomically with the first increment
  through Lua, suitable for distributed fixed-window rate limiting.
- Counter namespace isolation and independent expiry for each logical key.
- Redis-backed tests covering concurrent increments, independent keys,
  persistent counters, expiry behavior, and input validation.
- Rate-limiting guides and examples in the README and project website.

## 0.2.0 — 2026-07-24

### Added

- A synchronous `createDistributedMapWriter()` publisher for write-only
  processes.
- Writer-side `set`, `delete`, `clear`, `flush`, and `destroy` operations using
  the existing coalesced atomic Hash + Stream patch format.
- Zero-I/O writer startup with no local snapshot, duplicated Redis connection,
  XREAD loop, or synchronization timer.

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
- Versioned GitHub installs with automatic source builds and release artifacts.
- Dual ESM/CommonJS package output with TypeScript declarations.
