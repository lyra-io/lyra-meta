# Lyra Meta Instructions

`lyra-meta` is Lyra's shared metadata library. It currently exposes bootstrap and
component registration Protobuf contracts, bootstrap credential helpers, and typed,
Memory-backed initialization reads and ID reservation. Bootstrap writes, durable
storage, and registration operations are introduced separately.
Do not add a README. `CLAUDE.md` is the instruction entry point, and `AGENTS.md`
must remain a tracked relative symlink to it.

## Shared conventions

Before changing this repository, read these approved shared rules completely:

- [Workflow](https://github.com/lyra-io/conventions/blob/da3324a3806b6ea882192835583b2e8b9f13f28b/workflow.md)
- [Rust conventions](https://github.com/lyra-io/conventions/blob/da3324a3806b6ea882192835583b2e8b9f13f28b/rust.md)

The recorded conventions revision is `da3324a3806b6ea882192835583b2e8b9f13f28b`,
approved on `conventions/main`. Fetch the file contents or read them from a local
checkout at that revision; a link alone does not load the instructions. Report
unavailable rules and resolve conflicts explicitly. Update this pin through a
reviewed change; do not copy the shared policy here or silently change revisions.

## Layout and validation

- `src/lib.rs` is the library entry point; add modules only with their scoped implementation.
- `proto/pb_meta.proto` defines bootstrap and component registration wire contracts;
  `build.rs` generates Rust types into Cargo's `OUT_DIR`. Do not check generated
  output into source.
- `src/proto/mod.rs` exposes the generated types; `tests/protobuf.rs` checks
  field presence, wire tags, unknown fields, and malformed encoding.
- `tests/bootstrap_protobuf.rs` covers user/database/verifier wire
  contracts. Keep retained field numbers stable and do not reuse reserved tags.
  Database records contain only name, ID, and owner user ID. Introduce database
  management policies and lifecycle state with their separately reviewed behavior,
  not as bootstrap placeholders. Generated getters can hide absent options;
  inspect raw fields.
- `src/proto/redacted.rs` supplies redacted `Debug` implementations for `User`
  and `ScramSha256Verifier`; keep their generated debug output disabled in
  `build.rs`. Redaction does not sanitize field access or serialized bytes.
- `src/credentials` generates bootstrap SCRAM-SHA-256 verifiers and validates
  their structure. `make_verifier` and `validate_verifier` are synchronous and
  independent of storage. They do not implement login, password-file loading,
  password-strength policy, or initialization writes. Async callers must move
  derivation off executor workers. Errors must not carry credential contents.
- Credential generation uses `ring` primitives/OS randomness, `stringprep` for
  SASLprep, and `zeroize` for Lyra-owned normalized/salted password buffers.
  Retain PostgreSQL-style fallback for prohibited or empty normalization results.
  Do not claim all library temporaries, caller-owned input, or returned Protobuf
  fields are erased. Keep the current 4096-iteration work factor explicit;
  configurable work factors belong to a separately reviewed change.
- Credential unit tests cover the public RFC vector, normalization, and injected
  RNG failure. `tests/credentials.rs` covers the public API, byte/record bounds,
  no-repair validation, serialization, and redaction using synthetic inputs only.
- `src/metadata` owns the `Metadata` trait, typed errors, marker validation, and
  `MemoryMetadata`. Its current methods are `fetch_instance`, `is_initialized`,
  `allocate_database_id`, `allocate_user_id`, and `close`; add other methods only
  with their implementations and tests.
- Keep ID allocation directly in each metadata implementation, not in a separate
  allocator module or counter-transport abstraction. `MemoryMetadata` reads,
  checks, increments, and stores the four-byte counter under one write lock;
  it needs no transport revisions or retry loop. Never hold a guard across await.
- `tests/id_allocation.rs` checks separate domains, shared-client uniqueness, and
  close races. Memory unit tests cover exact keys/bytes, malformed/exhausted
  counters, lock poisoning, and preservation on error/close. Counters are not durable;
  construction starts an isolated empty namespace and dropping it loses its data.
- `tests/metadata.rs` checks the public trait-object and client-lifecycle contract.
  Memory unit tests inject raw records privately; do not expose a public marker
  setter that could bypass future bootstrap validation.
- `Cargo.toml` declares the crate; keep `Cargo.lock` tracked.
- `rust-toolchain.toml` pins Rust 1.92.0 with rustfmt and Clippy.
- `.github/workflows/ci.yml` checks the crate on Linux.
- Preserve `LICENSE`.

Install `protoc` before building (`brew install protobuf` on macOS or
`apt-get install protobuf-compiler` on Debian/Ubuntu). An alternate compiler can
be selected with `PROTOC`. Keep `prost` and `prost-build` on the same release line.
The current line is 0.13, matching the reference MVP. Generated optional-field
getters may hide absence; metadata validation must inspect the fields directly.

Run from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo package --locked
git diff --check
```

These checks cover wire contracts, credential helpers, and Memory read/allocation/
close behavior, not authentication, durable storage, bootstrap writes, or
registration. Typed initialization reads reject an unset flag even though raw
Protobuf decoding accepts it. Component validation will arrive with registration.
Tokio is currently a test-only dependency; metadata construction must require no
runtime or process-global setup. No Oxia, Docker, Kubernetes, real credentials,
or deployment is required for these checks.

## Implementation boundaries

- Introduce behavior in small, independently reviewable PRs following approved LIP-0000.
- [MVP PR #3](https://github.com/lyra-io/lyra-meta/pull/3) is reference material for
  splitting the implementation, not a change to merge wholesale or copy blindly.
- Add dependencies, schemas, backends, background workers, and observability only
  alongside the feature and its tests. Do not add speculative placeholder APIs.
- ID allocation stores exactly four big-endian bytes for a `u32` counter at
  `/catalog/allocator/user` and `/catalog/allocator/database`, not a Protobuf
  wrapper. Reject malformed values and overflow; never reset counters when
  records are deleted. IDs may have gaps after failed/cancelled operations.
  Durable storage/bootstrap integration is still separate: validate counter and
  object-ID consistency, and reject missing counters alongside existing records
  rather than using first-allocation behavior to repair an existing deployment.
  Implement and test Oxia-specific CAS, conflicts, and uncertain write outcomes
  directly in the Oxia metadata implementation when that backend is introduced.
- Keep metadata/storage operations separate from reusable configuration, manifest
  watching, and explicit opt-in process observability when those features arrive.
- Removing the previous API is intentional. Consumers must remain pinned to their
  existing implementation revisions until replacement APIs are reviewed; do not
  update consumers or deploy the scaffold as part of initialization.
- Never print or commit credentials, password files, verifiers, Secret payloads,
  kubeconfigs, or private proposal contents. Preserve retained clusters, volumes,
  and metadata. Future backend tests must use explicitly disposable namespaces.
