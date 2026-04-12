# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Ferry

Elixir library for in-memory operation queuing with batched processing, dead-letter queues, and telemetry. Zero runtime dependencies beyond `:telemetry`. Requires Elixir 1.17+.

## Commands

```bash
mix deps.get            # Install dependencies
mix compile             # Compile
mix test                # Run all tests
mix test test/ferry/server_test.exs           # Run a single test file
mix test test/ferry/server_test.exs:42        # Run a single test at line
mix format              # Auto-format code
mix format --check-formatted  # Check formatting
mix credo               # Lint
mix dialyzer            # Static type analysis (slow first run)
```

## Architecture

Ferry instances are defined with `use Ferry` and added to a supervision tree. Each instance runs its own supervision subtree:

```
InstanceSupervisor (:rest_for_one)
├── EtsHeir (only if persistence: :ets)
├── Ferry.Server (GenServer — queue, flush, resolver dispatch)
└── Ferry.StatsCollector (attaches to telemetry events, tracks per-instance stats)
```

**Operation lifecycle:** `:pending` → `:processing` → `:completed` | `:dead` (dead-letter queue)

### Key modules

- `lib/ferry.ex` — Public API and `use Ferry` macro (defines `child_spec/1` on the calling module)
- `lib/ferry/server.ex` — Core GenServer: queue management, batch flushing, resolver invocation, auto-flush timer
- `lib/ferry/operation.ex` — Operation struct with status tracking and timestamps
- `lib/ferry/batch.ex` — Batch struct passed to resolver functions
- `lib/ferry/store.ex` — Storage behaviour with two implementations:
  - `lib/ferry/store/memory.ex` — In-process queue (default)
  - `lib/ferry/store/ets.ex` — ETS-backed, crash-recoverable via `lib/ferry/ets_heir.ex`
- `lib/ferry/telemetry.ex` — Emits `[:ferry, ...]` telemetry events at every lifecycle point
- `lib/ferry/stats_collector.ex` — Per-instance telemetry listener that accumulates stats
- `lib/ferry/stats.ex` — Stats struct
- `lib/ferry/id_generator.ex` — Generates `fry_`-prefixed IDs

### Resolver contract

A resolver receives `%Ferry.Batch{}` and must return `[{operation_id, :ok | :error, result}]` for every operation. If it raises, times out, or returns unexpected values, the entire batch goes to the DLQ.

### Test helpers

`test/support/test_helpers.ex` provides factory resolvers (`success_resolver/0`, `failure_resolver/0`, `partial_resolver/0`, `crash_resolver/0`, `slow_resolver/1`) and `start_ferry/1` for isolated test instances. Tests use `async: true`.

## Example app

`examples/ferry_dashboard/` is a Phoenix LiveView dashboard. It has its own `mix.exs` and deps. Run with `mix setup && mix phx.server` from that directory.
