# Project Instructions

## Rust imports

- Import referenced types into scope and use their short names instead of repeating fully qualified paths. For example, prefer `use wal::WalError;` and `WalError::Io` over `crate::wal::WalError::Io`.

## Rust module layout

- Keep `mod.rs` files declarative. They may contain module declarations, exports and re-exports, interfaces, shared type declarations, and constants.
- Do not put operational logic or function implementations in `mod.rs`; place them in clearly named submodules instead.
- As an explicit exception, `wal/segment/mod.rs` may contain small segment namespace utilities such as path construction and directory listing or syncing.
- In `mod.rs`, place traits after module declarations, exports and re-exports, type aliases, and constants.

## Rust implementation helpers

- Use numbered suffixes for private implementation layers, such as `open0` and `open1`, instead of names such as `open_inner`.
- Use associated `Type::new` functions for type constructors.
- Reserve the `make_` prefix for free utilities that derive standalone values such as paths, names, or static-like strings, for example `make_segment_path`.
- Keep short, single-use logic inline instead of extracting a helper that is only several straightforward lines.

## Stateful Rust structs

- Group fields in this order: control state, immutable state, then mutable state.
- Add `// Control state`, `// Immutable state`, and `// Mutable state` comments to make the groups explicit.
- Within control state, declare an execution or cancellation context first and background task handles immediately after it.
- Follow the same field order in struct initializers when practical.

Example:

```rust
pub struct Service {
    // Control state
    context: CancellationToken,
    tasks: Mutex<Option<JoinSet<()>>>,

    // Immutable state
    request_tx: mpsc::Sender<Request>,
    options: ServiceOptions,

    // Mutable state
    state: Arc<RwLock<State>>,
}
```

## Repository workflow

- Use feature branches and pull requests; never push directly to the default branch.
- Never use a `codex/` branch prefix. Use `feat/`, `fix/`, `test/`, or `chore/`.
- Merges are squash-only. Do not merge or enable auto-merge without Mattison's request.
- Keep this MVP in one implementation PR per repository until review.
- CLAUDE.md is the source of repository instructions; AGENTS.md is a relative symlink to it.

## API naming

- Local fields: `identity()`, `name()`.
- Stored reads: `fetch_*()`; enumeration: `list_*()`.
- Persistence: `store_*()` with explicit conditional/overwrite semantics.
- Create-if-absent: `create_*()`; update-existing: `update_*()`.
- Lifecycle: `initialize()`, `register_catalog_component()`, `close()`.

## Ownership

Meta owns storage/lifecycle contracts, protobuf, shared configuration, the reusable manifest toolkit,
and opt-in observability. Constructing a metadata client must not initialize global telemetry.
Registration notifications and recovery remain private; no public lease handle/subscription.
The vendored pprof source retains upstream style; keep local patches narrowly documented.
