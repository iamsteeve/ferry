# Changelog

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
