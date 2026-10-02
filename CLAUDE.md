# Lyra Meta Instructions

`lyra-meta` is Lyra's shared metadata library. This branch establishes a clean,
dependency-free Rust scaffold; it does not implement metadata or runtime behavior.
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
- `Cargo.toml` declares the crate; keep `Cargo.lock` tracked.
- `rust-toolchain.toml` pins Rust 1.92.0 with rustfmt and Clippy.
- `.github/workflows/ci.yml` checks the scaffold on Linux.
- Preserve `LICENSE`.

Run from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo package --locked
git diff --check
```

The scaffold has no behavioral tests. A successful build is not evidence of
metadata correctness. No Oxia, protobuf compiler, Docker, Kubernetes, credentials,
or deployment is required for these checks.

## Implementation boundaries

- Introduce behavior in small, independently reviewable PRs following approved LIP-0000.
- [MVP PR #3](https://github.com/lyra-io/lyra-meta/pull/3) is reference material for
  splitting the implementation, not a change to merge wholesale or copy blindly.
- Add dependencies, schemas, backends, background workers, and observability only
  alongside the feature and its tests. Do not add speculative placeholder APIs.
- Keep metadata/storage operations separate from reusable configuration, manifest
  watching, and explicit opt-in process observability when those features arrive.
- Removing the previous API is intentional. Consumers must remain pinned to their
  existing implementation revisions until replacement APIs are reviewed; do not
  update consumers or deploy the scaffold as part of initialization.
- Never print or commit credentials, password files, verifiers, Secret payloads,
  kubeconfigs, or private proposal contents. Preserve retained clusters, volumes,
  and metadata. Future backend tests must use explicitly disposable namespaces.
