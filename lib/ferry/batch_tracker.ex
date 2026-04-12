defmodule Ferry.BatchTracker do
  @moduledoc """
  Opt-in batch history tracker that listens to telemetry events and maintains
  a bounded, purgeable record of executed batches.

  Enabled by setting `batch_tracking: true` in the Ferry instance configuration.

  ## Configuration

  | Option | Default | Description |
  |---|---|---|
  | `max_batch_history` | `1_000` | Max batch records to retain |
  | `batch_history_ttl` | `3_600_000` | TTL for batch records (ms), `:infinity` to disable |
  """

  use GenServer

  require Logger

  alias Ferry.BatchRecord

  @purge_interval :timer.seconds(30)

  # Client API

  def start_link(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)
    GenServer.start_link(__MODULE__, opts, name: via(ferry_name))
  end

  @doc """
  List recent batch history records.

  ## Options

    * `:limit` — max records to return (default: 50)
    * `:status` — filter by batch status
  """
  def batch_history(ferry_name, opts \\ []) do
    GenServer.call(via(ferry_name), {:batch_history, opts})
  catch
    :exit, {:noproc, _} -> {:error, :not_enabled}
  end

  @doc """
  Get details for a specific batch by ID.
  """
  def batch_info(ferry_name, batch_id) do
    GenServer.call(via(ferry_name), {:batch_info, batch_id})
  catch
    :exit, {:noproc, _} -> {:error, :not_enabled}
  end

  @doc """
  Manually purge all batch history.
  """
  def purge_history(ferry_name) do
    GenServer.call(via(ferry_name), :purge_history)
  catch
    :exit, {:noproc, _} -> {:error, :not_enabled}
  end

  # Server callbacks

  @impl true
  def init(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)
    handler_id = "ferry_batch_tracker_#{ferry_name}"

    events = [
      [:ferry, :batch, :started],
      [:ferry, :batch, :completed],
      [:ferry, :batch, :timeout],
      [:ferry, :batch, :crashed]
    ]

    :telemetry.attach_many(
      handler_id,
      events,
      &__MODULE__.handle_event/4,
      %{ferry_name: ferry_name}
    )

    max_history = Keyword.get(opts, :max_batch_history, 1_000)
    history_ttl = Keyword.get(opts, :batch_history_ttl, :timer.hours(1))

    schedule_purge(history_ttl)

    {:ok,
     %{
       ferry_name: ferry_name,
       handler_id: handler_id,
       records: %{},
       history: [],
       max_history: max_history,
       history_ttl: history_ttl
     }}
  end

  @impl true
  def terminate(_reason, state) do
    :telemetry.detach(state.handler_id)
    :ok
  end

  # Telemetry handler — runs in the emitting process (Ferry.Server),
  # so we cast to avoid blocking the server.

  @doc false
  def handle_event(_event, _measurements, %{ferry: ferry}, %{ferry_name: name})
      when ferry != name,
      do: :ok

  def handle_event([:ferry, :batch, :started], measurements, metadata, config) do
    GenServer.cast(via(config.ferry_name), {:batch_started, measurements, metadata})
  end

  def handle_event([:ferry, :batch, :completed], measurements, metadata, config) do
    GenServer.cast(via(config.ferry_name), {:batch_completed, measurements, metadata})
  end

  def handle_event([:ferry, :batch, :timeout], measurements, metadata, config) do
    GenServer.cast(via(config.ferry_name), {:batch_timeout, measurements, metadata})
  end

  def handle_event([:ferry, :batch, :crashed], measurements, metadata, config) do
    GenServer.cast(via(config.ferry_name), {:batch_crashed, measurements, metadata})
  end

  # Cast handlers

  @impl true
  def handle_cast({:batch_started, measurements, metadata}, state) do
    record = %BatchRecord{
      id: metadata.batch_id,
      operation_ids: Map.get(metadata, :operation_ids, []),
      size: measurements.batch_size,
      trigger: metadata.trigger,
      status: :started,
      started_at: DateTime.utc_now()
    }

    records = Map.put(state.records, record.id, record)
    history = [record.id | state.history]

    # Trim to max_history
    {history, records} = trim_history(history, records, state.max_history)

    {:noreply, %{state | records: records, history: history}}
  end

  @impl true
  def handle_cast({:batch_completed, measurements, metadata}, state) do
    batch_id = metadata.batch_id
    status = Map.get(metadata, :status_override) || :completed

    case Map.fetch(state.records, batch_id) do
      {:ok, record} ->
        updated = %{
          record
          | status: status,
            completed_at: DateTime.utc_now(),
            duration_ms: measurements.duration_ms,
            succeeded: measurements.succeeded,
            failed: measurements.failed
        }

        {:noreply, %{state | records: Map.put(state.records, batch_id, updated)}}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:batch_timeout, measurements, metadata}, state) do
    batch_id = metadata.batch_id

    case Map.fetch(state.records, batch_id) do
      {:ok, record} ->
        updated = %{
          record
          | status: :timeout,
            completed_at: DateTime.utc_now(),
            duration_ms: measurements.timeout_ms,
            failed: measurements.batch_size
        }

        {:noreply, %{state | records: Map.put(state.records, batch_id, updated)}}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:batch_crashed, measurements, metadata}, state) do
    batch_id = metadata.batch_id

    case Map.fetch(state.records, batch_id) do
      {:ok, record} ->
        updated = %{
          record
          | status: :crashed,
            completed_at: DateTime.utc_now(),
            failed: measurements.batch_size
        }

        {:noreply, %{state | records: Map.put(state.records, batch_id, updated)}}

      :error ->
        {:noreply, state}
    end
  end

  # Call handlers

  @impl true
  def handle_call({:batch_history, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 50)
    status_filter = Keyword.get(opts, :status)

    results =
      state.history
      |> Enum.map(&Map.get(state.records, &1))
      |> Enum.reject(&is_nil/1)
      |> maybe_filter_status(status_filter)
      |> Enum.take(limit)

    {:reply, results, state}
  end

  @impl true
  def handle_call({:batch_info, batch_id}, _from, state) do
    case Map.fetch(state.records, batch_id) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:purge_history, _from, state) do
    {:reply, :ok, %{state | records: %{}, history: []}}
  end

  # Purge timer

  @impl true
  def handle_info(:purge_expired, state) do
    state = do_purge_expired(state)
    schedule_purge(state.history_ttl)
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private helpers

  defp via(ferry_name), do: :"#{ferry_name}.BatchTracker"

  defp schedule_purge(:infinity), do: :ok

  defp schedule_purge(_ttl) do
    Process.send_after(self(), :purge_expired, @purge_interval)
  end

  defp trim_history(history, records, max) when length(history) > max do
    {keep, drop} = Enum.split(history, max)
    trimmed_records = Enum.reduce(drop, records, fn id, acc -> Map.delete(acc, id) end)
    {keep, trimmed_records}
  end

  defp trim_history(history, records, _max), do: {history, records}

  defp do_purge_expired(%{history_ttl: :infinity} = state), do: state

  defp do_purge_expired(state) do
    now = DateTime.utc_now()

    {keep_ids, drop_ids} =
      Enum.split_with(state.history, fn id ->
        case Map.fetch(state.records, id) do
          {:ok, record} ->
            ts = record.completed_at || record.started_at
            DateTime.diff(now, ts, :millisecond) <= state.history_ttl

          :error ->
            false
        end
      end)

    trimmed_records = Enum.reduce(drop_ids, state.records, fn id, acc -> Map.delete(acc, id) end)
    %{state | records: trimmed_records, history: keep_ids}
  end

  defp maybe_filter_status(records, nil), do: records

  defp maybe_filter_status(records, status) do
    Enum.filter(records, &(&1.status == status))
  end
end
