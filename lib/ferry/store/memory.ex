defmodule Ferry.Store.Memory do
  @moduledoc """
  In-memory store implementation using `:queue` and maps.

  Fastest option. State is lost on process crash.
  """

  @behaviour Ferry.Store

  defstruct queue: :queue.new(),
            completed: %{},
            dlq: %{},
            index: %{}

  @impl true
  def init(_ferry_name, _opts) do
    {:ok, %__MODULE__{}}
  end

  @impl true
  def push(state, %Ferry.Operation{} = op) do
    state =
      state
      |> put_in_queue(op)
      |> put_in_index(op)

    {:ok, state}
  end

  @impl true
  def push_many(state, operations) do
    state =
      Enum.reduce(operations, state, fn op, acc ->
        acc
        |> put_in_queue(op)
        |> put_in_index(op)
      end)

    {:ok, state}
  end

  @impl true
  def pop_batch(state, count) do
    {ops, new_queue} = pop_n(state.queue, count, [])

    updated_index =
      Enum.reduce(ops, state.index, fn op, idx ->
        Map.put(idx, op.id, %{op | status: :processing})
      end)

    ops_with_status = Enum.map(ops, &%{&1 | status: :processing})
    {ops_with_status, %{state | queue: new_queue, index: updated_index}}
  end

  @impl true
  def get(state, id) do
    case Map.fetch(state.index, id) do
      {:ok, op} -> {:ok, op}
      :error -> {:error, :not_found}
    end
  end

  @impl true
  def mark_completed(state, id, result, completed_at, batch_id) do
    case Map.fetch(state.index, id) do
      {:ok, op} ->
        updated = %{
          op
          | status: :completed,
            result: result,
            completed_at: completed_at,
            batch_id: batch_id
        }

        state = %{
          state
          | completed: Map.put(state.completed, id, updated),
            index: Map.put(state.index, id, updated)
        }

        {:ok, state}

      :error ->
        {:ok, state}
    end
  end

  @impl true
  def mark_failed(state, id, error, completed_at, batch_id) do
    case Map.fetch(state.index, id) do
      {:ok, op} ->
        updated = %{
          op
          | status: :dead,
            error: error,
            completed_at: completed_at,
            batch_id: batch_id
        }

        state = %{
          state
          | dlq: Map.put(state.dlq, id, updated),
            index: Map.put(state.index, id, updated)
        }

        {:ok, state}

      :error ->
        {:ok, state}
    end
  end

  @impl true
  def move_to_dlq(state, id, error) do
    case Map.fetch(state.index, id) do
      {:ok, op} ->
        updated = %{op | status: :dead, error: error}

        state = %{
          state
          | dlq: Map.put(state.dlq, id, updated),
            index: Map.put(state.index, id, updated)
        }

        {:ok, state}

      :error ->
        {:ok, state}
    end
  end

  @impl true
  def move_batch_to_dlq(state, operations, error) do
    Enum.reduce(operations, {:ok, state}, fn op, {:ok, acc} ->
      updated = %{op | status: :dead, error: error}

      {:ok,
       %{
         acc
         | dlq: Map.put(acc.dlq, op.id, updated),
           index: Map.put(acc.index, op.id, updated)
       }}
    end)
  end

  @impl true
  def list_pending(state, opts) do
    limit = Keyword.get(opts, :limit, 50)

    state.queue
    |> :queue.to_list()
    |> Enum.take(limit)
  end

  @impl true
  def list_completed(state, opts) do
    limit = Keyword.get(opts, :limit, 50)

    state.completed
    |> Map.values()
    |> Enum.sort_by(& &1.completed_at, {:desc, DateTime})
    |> Enum.take(limit)
  end

  @impl true
  def list_dlq(state) do
    Map.values(state.dlq)
  end

  @impl true
  def dlq_size(state) do
    map_size(state.dlq)
  end

  @impl true
  def retry_all_dlq(state) do
    dlq_ops =
      state.dlq
      |> Map.values()
      |> Enum.map(&%{&1 | status: :pending, error: nil})
      |> Enum.sort_by(& &1.order)

    count = length(dlq_ops)

    new_queue =
      Enum.reduce(dlq_ops, state.queue, fn op, q ->
        :queue.in(op, q)
      end)

    updated_index =
      Enum.reduce(dlq_ops, state.index, fn op, idx ->
        Map.put(idx, op.id, op)
      end)

    {count, %{state | dlq: %{}, queue: new_queue, index: updated_index}}
  end

  @impl true
  def drain_dlq(state) do
    count = map_size(state.dlq)

    updated_index =
      Enum.reduce(state.dlq, state.index, fn {id, _op}, idx ->
        Map.delete(idx, id)
      end)

    {count, %{state | dlq: %{}, index: updated_index}}
  end

  @impl true
  def queue_size(state) do
    :queue.len(state.queue)
  end

  @impl true
  def completed_size(state) do
    map_size(state.completed)
  end

  @impl true
  def purge_completed(state, max_age_ms, max_count) do
    now = DateTime.utc_now()

    # Purge by TTL
    {expired_ids, remaining} =
      Enum.split_with(state.completed, fn {_id, op} ->
        case op.completed_at do
          nil ->
            false

          completed_at ->
            DateTime.diff(now, completed_at, :millisecond) > max_age_ms
        end
      end)

    remaining_map = Map.new(remaining)
    expired_id_set = MapSet.new(expired_ids, fn {id, _} -> id end)

    # Purge by count
    {over_limit_ids, final_completed} =
      if map_size(remaining_map) > max_count do
        sorted =
          remaining_map
          |> Map.values()
          |> Enum.sort_by(& &1.completed_at, {:asc, DateTime})

        to_remove = Enum.take(sorted, map_size(remaining_map) - max_count)
        remove_ids = MapSet.new(to_remove, & &1.id)

        final = Map.reject(remaining_map, fn {id, _} -> MapSet.member?(remove_ids, id) end)
        {remove_ids, final}
      else
        {MapSet.new(), remaining_map}
      end

    all_purged_ids = MapSet.union(expired_id_set, over_limit_ids)
    purged_count = MapSet.size(all_purged_ids)

    updated_index =
      Enum.reduce(all_purged_ids, state.index, fn id, idx ->
        Map.delete(idx, id)
      end)

    {purged_count, %{state | completed: final_completed, index: updated_index}}
  end

  @impl true
  def clear_queue(state, error) do
    ops = :queue.to_list(state.queue)
    count = length(ops)

    {dlq, index} =
      Enum.reduce(ops, {state.dlq, state.index}, fn op, {d, idx} ->
        updated = %{op | status: :dead, error: error}
        {Map.put(d, op.id, updated), Map.put(idx, op.id, updated)}
      end)

    {count, %{state | queue: :queue.new(), dlq: dlq, index: index}}
  end

  @impl true
  def memory_bytes(state) do
    :erlang.external_size(state)
  end

  # Private helpers

  defp put_in_queue(state, op) do
    %{state | queue: :queue.in(op, state.queue)}
  end

  defp put_in_index(state, op) do
    %{state | index: Map.put(state.index, op.id, op)}
  end

  defp pop_n(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp pop_n(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, item}, new_queue} -> pop_n(new_queue, n - 1, [item | acc])
      {:empty, queue} -> {Enum.reverse(acc), queue}
    end
  end
end
