# Bounded CPU capture storage

Source: pprof 0.15.0, published by the TiKV project under Apache-2.0.
The original license and source are retained. This local dependency is owned by
Meta so executables do not need a workspace-wide Cargo patch.

Upstream bounds its in-memory hash buckets but spills evicted stacks to an
unbounded temporary file. It also discards errors from the signal-handler
collector. Lyra caps that spill at 16 MiB and latches every collector failure.
Report generation then fails instead of returning an incomplete successful
profile. The wrapper independently caps encoded and compressed output at 16 MiB.

Only src/collector.rs is changed. Remove this vendored copy when upstream offers
an equivalent bounded, fail-closed capture contract. No symbols or profiling
artifacts are stored in this repository.
