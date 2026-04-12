defmodule Ferry do
  @moduledoc """
  In-memory operation queue with batched processing, dead-letter queues, and telemetry.

  Ferry buffers external operations in-memory, processes them in configurable batches
  via a user-defined resolver function, and provides full operation lifecycle tracking.

  ## Quick Start

      # 1. Define your resolver
      defmodule MyResolver do
        def resolve(%Ferry.Batch{operations: ops}) do
          Enum.map(ops, fn op ->
            case process(op.payload) do
              {:ok, result} -> {op.id, :ok, result}
              {:error, reason} -> {op.id, :error, reason}
            end
          end)
        end
      end

      # 2. Define your Ferry
      defmodule MyFerry do
        use Ferry,
          resolver: &MyResolver.resolve/1,
          batch_size: 25,
          flush_interval: 5_000
      end

      # 3. Add to supervision tree
      children = [MyFerry]
      Supervisor.start_link(children, strategy: :one_for_one)

      # 4. Push operations
      {:ok, id} = Ferry.push(MyFerry, %{data: "hello"})
      {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(MyFerry, id)

  ## Configuration

  | Option | Default | Description |
  |---|---|---|
  | `name` | *required* | Atom identifier for the Ferry instance |
  | `resolver` | *required* | Function that processes a `%Ferry.Batch{}` |
  | `batch_size` | `10` | Max operations per batch flush |
  | `flush_interval` | `5_000` | Milliseconds between auto-flushes |
  | `max_queue_size` | `10_000` | Max pending operations (back-pressure) |
  | `auto_flush` | `true` | Whether timer-based flushing is active |
  | `operation_timeout` | `300_000` | Max ms for resolver execution |
  | `max_completed` | `1_000` | Max completed operations in history |
  | `completed_ttl` | `1_800_000` | TTL for completed operations (ms) |
  | `persistence` | `:memory` | `:memory` or `:ets` |
  | `id_generator` | `&Ferry.IdGenerator.generate/1` | Custom ID generator |
  | `batch_tracking` | `false` | Enable batch history tracking |
  | `max_batch_history` | `1_000` | Max batch records to retain |
  | `batch_history_ttl` | `3_600_000` | TTL for batch records (ms) |
  """

  @type ferry_name :: atom()

  # ── use Ferry macro ──

  @doc """
  Generates `child_spec/1` and `start_link/1` for clean supervision tree integration.

  The module name becomes the default ferry name.

  ## Example

      defmodule MyApp.ApiFerry do
        use Ferry,
          resolver: &MyApp.ApiResolver.resolve/1,
          batch_size: 25,
          flush_interval: :timer.seconds(5)
      end

      # In your supervision tree:
      children = [MyApp.ApiFerry]
  """
  defmacro __using__(opts) do
    quote do
      def child_spec(override_opts \\ []) do
        merged = Keyword.merge(unquote(opts), override_opts)
        merged = Keyword.put_new(merged, :name, __MODULE__)

        %{
          id: __MODULE__,
          start: {Ferry, :start_link, [merged]},
          type: :supervisor
        }
      end

      def start_link(override_opts \\ []) do
        merged = Keyword.merge(unquote(opts), override_opts)
        merged = Keyword.put_new(merged, :name, __MODULE__)
        Ferry.start_link(merged)
      end

      defoverridable child_spec: 1, start_link: 1
    end
  end

  # ── Lifecycle ──

  @doc """
  Starts a Ferry instance under its own supervision tree.

  ## Required options

    * `:name` — atom identifier for the instance
    * `:resolver` — function that processes a `%Ferry.Batch{}`

  See module documentation for all available options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Ferry.InstanceSupervisor.start_link(opts)
  end

  @doc """
  Returns a child specification for starting under a supervisor.
  """
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  # ── Push operations ──

  @doc """
  Push a single operation. Returns `{:ok, id}` or `{:error, :queue_full}`.
  """
  @spec push(ferry_name(), term()) :: {:ok, String.t()} | {:error, :queue_full}
  def push(ferry_name, payload) do
    Ferry.Server.push(ferry_name, payload)
  end

  @doc """
  Push multiple operations atomically.

  All-or-nothing: if the queue can't fit all operations, none are pushed.
  """
  @spec push_many(ferry_name(), [term()]) :: {:ok, [String.t()]} | {:error, :queue_full}
  def push_many(ferry_name, payloads) do
    Ferry.Server.push_many(ferry_name, payloads)
  end

  # ── Query ──

  @doc """
  Get operation by ID. Returns `{:error, :not_found}` if purged or never existed.
  """
  @spec status(ferry_name(), String.t()) :: {:ok, Ferry.Operation.t()} | {:error, :not_found}
  def status(ferry_name, operation_id) do
    Ferry.Server.status(ferry_name, operation_id)
  end

  @doc """
  Get current stats snapshot.
  """
  @spec stats(ferry_name()) :: Ferry.Stats.t()
  def stats(ferry_name) do
    Ferry.Server.stats(ferry_name)
  end

  @doc """
  Get current queue size.
  """
  @spec queue_size(ferry_name()) :: non_neg_integer()
  def queue_size(ferry_name) do
    Ferry.Server.queue_size(ferry_name)
  end

  @doc """
  Get the runtime configuration for this Ferry instance.
  """
  @spec config(ferry_name()) :: map()
  def config(ferry_name) do
    Ferry.Server.config(ferry_name)
  end

  @doc """
  Get multiple operations by ID. Returns a map of `%{id => {:ok, Operation} | {:error, :not_found}}`.
  """
  @spec status_many(ferry_name(), [String.t()]) :: %{
          String.t() => {:ok, Ferry.Operation.t()} | {:error, :not_found}
        }
  def status_many(ferry_name, operation_ids) do
    Ferry.Server.status_many(ferry_name, operation_ids)
  end

  # ── Flow control ──

  @doc """
  Trigger immediate flush. Synchronous — blocks the caller until the resolver
  finishes processing the batch (subject to `operation_timeout`).
  """
  @spec flush(ferry_name()) :: :ok | {:error, :empty_queue}
  def flush(ferry_name) do
    Ferry.Server.flush(ferry_name)
  end

  @doc """
  Pause auto-flush timer. Manual flush still works.
  """
  @spec pause(ferry_name()) :: :ok
  def pause(ferry_name) do
    Ferry.Server.pause(ferry_name)
  end

  @doc """
  Resume auto-flush timer.
  """
  @spec resume(ferry_name()) :: :ok
  def resume(ferry_name) do
    Ferry.Server.resume(ferry_name)
  end

  # ── Dead Letter Queue ──

  @doc """
  List all dead-lettered operations.
  """
  @spec dead_letters(ferry_name()) :: [Ferry.Operation.t()]
  def dead_letters(ferry_name) do
    Ferry.Server.dead_letters(ferry_name)
  end

  @doc """
  Count dead-lettered operations.
  """
  @spec dead_letter_count(ferry_name()) :: non_neg_integer()
  def dead_letter_count(ferry_name) do
    Ferry.Server.dead_letter_count(ferry_name)
  end

  @doc """
  Move all dead letters back to the pending queue for re-processing.
  """
  @spec retry_dead_letters(ferry_name()) :: {:ok, non_neg_integer()}
  def retry_dead_letters(ferry_name) do
    Ferry.Server.retry_dead_letters(ferry_name)
  end

  @doc """
  Permanently discard all dead letters.
  """
  @spec drain_dead_letters(ferry_name()) :: :ok
  def drain_dead_letters(ferry_name) do
    Ferry.Server.drain_dead_letters(ferry_name)
  end

  @doc """
  Clear all pending operations from the queue.

  Pending operations are moved to the Dead Letter Queue with error `:canceled`.
  Returns `{:ok, count}` with the number of cleared operations.
  """
  @spec clear(ferry_name()) :: {:ok, non_neg_integer()}
  def clear(ferry_name) do
    Ferry.Server.clear(ferry_name)
  end

  # ── Introspection ──

  # ── Batch History ──

  @doc """
  List recent batch history records.

  Requires `batch_tracking: true` in instance config.

  ## Options

    * `:limit` — max records to return (default: 50)
    * `:status` — filter by batch status (`:completed`, `:timeout`, etc.)
  """
  @spec batch_history(ferry_name(), keyword()) :: [Ferry.BatchRecord.t()] | {:error, :not_enabled}
  def batch_history(ferry_name, opts \\ []) do
    Ferry.BatchTracker.batch_history(ferry_name, opts)
  end

  @doc """
  Get details for a specific batch by ID.

  Requires `batch_tracking: true` in instance config.
  """
  @spec batch_info(ferry_name(), String.t()) ::
          {:ok, Ferry.BatchRecord.t()} | {:error, :not_found} | {:error, :not_enabled}
  def batch_info(ferry_name, batch_id) do
    Ferry.BatchTracker.batch_info(ferry_name, batch_id)
  end

  @doc """
  Manually purge all batch history.

  Requires `batch_tracking: true` in instance config.
  """
  @spec purge_batch_history(ferry_name()) :: :ok | {:error, :not_enabled}
  def purge_batch_history(ferry_name) do
    Ferry.BatchTracker.purge_history(ferry_name)
  end

  # ── Introspection ──

  @doc """
  List pending operations.

  ## Options

    * `:limit` — max operations to return (default: 50)
  """
  @spec pending(ferry_name(), keyword()) :: [Ferry.Operation.t()]
  def pending(ferry_name, opts \\ [limit: 50]) do
    Ferry.Server.pending(ferry_name, opts)
  end

  @doc """
  List completed operations.

  ## Options

    * `:limit` — max operations to return (default: 50)
  """
  @spec completed(ferry_name(), keyword()) :: [Ferry.Operation.t()]
  def completed(ferry_name, opts \\ [limit: 50]) do
    Ferry.Server.completed(ferry_name, opts)
  end
end
