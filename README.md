<img src="logo.svg" alt="Lyra" width="180" align="left">

<h3>lyra-meta</h3>

<p>Shared contracts for the Lyra streaming system.</p>

[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange?style=flat-square&logo=rust)](https://www.rust-lang.org)

<br clear="left">

## What this is

`lyra-meta` is a **library only** — it has no server and no CLI. It is the single
crate every other Lyra repo depends on:

| module     | contents                                                       |
|------------|----------------------------------------------------------------|
| `auth`     | SCRAM / basic authentication, identity                          |
| `metadata` | metadata interface, in-memory impl, Oxia-backed impl            |
| `proto`    | shared catalog message types (`io.lyra.proto.catalog.v1`)       |
| `utils`    | logging, promises, directory locks                              |

The `metadata` module is a *client* of an external [Oxia](https://github.com/oxia-db/oxia)
server; it does not run a service of its own.

## Consumers

- [lyra-catalog](https://github.com/lyra-io/lyra-catalog) — SQL parsing and planning, pgwire
- [lyra-stream](https://github.com/lyra-io/lyra-stream) — streaming storage
- [lyra-func](https://github.com/lyra-io/lyra-func) — function execution

## Use

```toml
[dependencies]
lyra-meta = { git = "https://github.com/lyra-io/lyra-meta", branch = "main" }
```

## Build

```sh
cargo build
cargo test
```

Requires `protoc` on the path.
