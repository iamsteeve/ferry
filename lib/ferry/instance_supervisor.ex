defmodule Ferry.InstanceSupervisor do
  @moduledoc """
  Per-instance supervisor that manages the Ferry.Server, StatsCollector,
  and optionally the EtsHeir process.
  """

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}.Supervisor")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    persistence = Keyword.get(opts, :persistence, :memory)
    batch_tracking = Keyword.get(opts, :batch_tracking, false)

    children =
      maybe_ets_heir(persistence, name) ++
        [
          {Ferry.Server, opts},
          {Ferry.StatsCollector, [ferry_name: name]}
        ] ++
        maybe_batch_tracker(batch_tracking, name, opts)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp maybe_batch_tracker(true, name, opts) do
    tracker_opts = [
      ferry_name: name,
      max_batch_history: Keyword.get(opts, :max_batch_history, 1_000),
      batch_history_ttl: Keyword.get(opts, :batch_history_ttl, :timer.hours(1))
    ]

    [{Ferry.BatchTracker, tracker_opts}]
  end

  defp maybe_batch_tracker(_false, _name, _opts), do: []

  defp maybe_ets_heir(:ets, name) do
    [{Ferry.EtsHeir, [ferry_name: name]}]
  end

  defp maybe_ets_heir(_persistence, _name), do: []
end
