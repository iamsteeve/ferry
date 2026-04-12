defmodule Ferry.StatsCollector do
  @moduledoc """
  Attaches `:telemetry` handlers scoped to a specific Ferry instance
  and maintains running counters and moving averages.

  Stats are queryable via `Ferry.stats/1`, which delegates to the Server
  (which holds its own counters). This collector exists as a separate
  process for future extension (e.g., time-series aggregation).
  """

  use GenServer

  def start_link(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)
    GenServer.start_link(__MODULE__, opts, name: via(ferry_name))
  end

  @impl true
  def init(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)
    handler_id = "ferry_stats_#{ferry_name}"

    events = [
      [:ferry, :operation, :pushed],
      [:ferry, :operation, :completed],
      [:ferry, :operation, :failed],
      [:ferry, :operation, :dead_lettered],
      [:ferry, :operation, :rejected],
      [:ferry, :batch, :started],
      [:ferry, :batch, :completed],
      [:ferry, :batch, :timeout],
      [:ferry, :queue, :paused],
      [:ferry, :queue, :resumed],
      [:ferry, :dlq, :retried],
      [:ferry, :dlq, :drained],
      [:ferry, :completed, :purged]
    ]

    :telemetry.attach_many(
      handler_id,
      events,
      &__MODULE__.handle_event/4,
      %{ferry_name: ferry_name}
    )

    {:ok, %{ferry_name: ferry_name, handler_id: handler_id}}
  end

  @impl true
  def terminate(_reason, state) do
    :telemetry.detach(state.handler_id)
    :ok
  end

  @doc false
  def handle_event(_event, _measurements, _metadata, _config) do
    # Events are processed by telemetry handlers.
    # Stats are maintained directly in Ferry.Server state for simplicity.
    # This handler exists as an extension point for external metrics systems.
    :ok
  end

  defp via(ferry_name) do
    :"#{ferry_name}.StatsCollector"
  end
end
