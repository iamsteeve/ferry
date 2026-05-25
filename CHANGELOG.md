# Changelog

## Unreleased

- `Ferry.Stats` exposes a new `memory_bytes` field with the approximate
  footprint of the underlying store. The example dashboard surfaces it.
- New public functions `Ferry.drain_completed/1` and `Ferry.delete/2` for
  forcing an immediate wipe of completed history and removing a single
  operation by ID from the queue, completed history, or DLQ.
- `Ferry.Server` now hibernates when its queue drains, releasing heap back
  to the runtime after load spikes. No API change.
- Terminal operations (`:completed`, `:dead`) keep a compact "lite"
  projection in the index instead of the full operation struct, cutting
  steady-state memory. Public API and return shapes are unchanged —
  results are hydrated transparently on read.

## 0.1.0

- Initial release
- Core queue engine with push, flush, back-pressure
- Dead Letter Queue with inspect/retry/drain
- Store behaviour with Memory and ETS backends
- ETS persistence with heir process for crash recovery
- Telemetry events at every lifecycle point
- Stats collector with queryable metrics
- `use Ferry` macro for module-based definition
- Pause/resume auto-flush
- Completed operation TTL auto-purge
