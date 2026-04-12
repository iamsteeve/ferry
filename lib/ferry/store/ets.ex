defmodule Ferry.Store.Ets do
  @moduledoc """
  ETS-backed store implementation.

  Survives process crashes via a heir process that holds the ETS tables.
  Uses four tables per instance: queue, dlq, completed, and an index for O(1) lookups by ID.
  """

  @behaviour Ferry.Store

  defstruct [:ferry_name, :queue_table, :dlq_table, :completed_table, :index_table]

  @impl true
  def init(ferry_name, _opts) do
    queue_table = table_name(ferry_name, :queue)
    dlq_table = table_name(ferry_name, :dlq)
    completed_table = table_name(ferry_name, :completed)
    index_table = table_name(ferry_name, :index)

    ensure_table(queue_table, :ordered_set)
    ensure_table(dlq_table, :set)
    ensure_table(completed_table, :ordered_set)
    ensure_table(index_table, :set)

    state = %__MODULE__{
      ferry_name: ferry_name,
      queue_table: queue_table,
      dlq_table: dlq_table,
      completed_table: completed_table,
      index_table: index_table
    }

    {:ok, state}
  end

  @impl true
  def push(state, %Ferry.Operation{} = op) do
    :ets.insert(state.queue_table, {{op.order, op.id}, op})
    :ets.insert(state.index_table, {op.id, op})
    {:ok, state}
  end

  @impl true
  def push_many(state, operations) do
    queue_records = Enum.map(operations, fn op -> {{op.order, op.id}, op} end)
    index_records = Enum.map(operations, fn op -> {op.id, op} end)
    :ets.insert(state.queue_table, queue_records)
    :ets.insert(state.index_table, index_records)
    {:ok, state}
  end

  @impl true
  def pop_batch(state, count) do
    ops = take_first_n(state.queue_table, count)

    updated_ops =
      Enum.map(ops, fn op ->
        :ets.delete(state.queue_table, {op.order, op.id})
        updated = %{op | status: :processing}
        :ets.insert(state.index_table, {op.id, updated})
        updated
      end)

    {updated_ops, state}
  end

  @impl true
  def get(state, id) do
    case :ets.lookup(state.index_table, id) do
      [{^id, op}] -> {:ok, op}
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def mark_completed(state, id, result, completed_at, batch_id) do
    case :ets.lookup(state.index_table, id) do
      [{^id, op}] ->
        updated = %{
          op
          | status: :completed,
            result: result,
            completed_at: completed_at,
            batch_id: batch_id
        }

        ts = DateTime.to_unix(completed_at, :microsecond)
        :ets.insert(state.completed_table, {{ts, id}, updated})
        :ets.insert(state.index_table, {id, updated})
        delete_from_queue_by_id(state.queue_table, id, op.order)
        :ets.delete(state.dlq_table, id)
        {:ok, state}

      [] ->
        {:ok, state}
    end
  end

  @impl true
  def mark_failed(state, id, error, completed_at, batch_id) do
    case :ets.lookup(state.index_table, id) do
      [{^id, op}] ->
        updated = %{
          op
          | status: :dead,
            error: error,
            completed_at: completed_at,
            batch_id: batch_id
        }

        :ets.insert(state.dlq_table, {id, updated})
        :ets.insert(state.index_table, {id, updated})
        delete_from_queue_by_id(state.queue_table, id, op.order)
        {:ok, state}

      [] ->
        {:ok, state}
    end
  end

  @impl true
  def move_to_dlq(state, id, error) do
    case :ets.lookup(state.index_table, id) do
      [{^id, op}] ->
        updated = %{op | status: :dead, error: error}
        :ets.insert(state.dlq_table, {id, updated})
        :ets.insert(state.index_table, {id, updated})
        delete_from_queue_by_id(state.queue_table, id, op.order)
        {:ok, state}

      [] ->
        {:ok, state}
    end
  end

  @impl true
  def move_batch_to_dlq(state, operations, error) do
    Enum.each(operations, fn op ->
      updated = %{op | status: :dead, error: error}
      :ets.insert(state.dlq_table, {op.id, updated})
      :ets.insert(state.index_table, {op.id, updated})
      delete_from_queue_by_id(state.queue_table, op.id, op.order)
    end)

    {:ok, state}
  end

  @impl true
  def list_pending(state, opts) do
    limit = Keyword.get(opts, :limit, 50)
    take_first_n(state.queue_table, limit)
  end

  @impl true
  def list_completed(state, opts) do
    limit = Keyword.get(opts, :limit, 50)

    state.completed_table
    |> :ets.tab2list()
    |> Enum.map(fn {_key, op} -> op end)
    |> Enum.sort_by(& &1.completed_at, {:desc, DateTime})
    |> Enum.take(limit)
  end

  @impl true
  def list_dlq(state) do
    :ets.tab2list(state.dlq_table)
    |> Enum.map(fn {_id, op} -> op end)
  end

  @impl true
  def dlq_size(state) do
    :ets.info(state.dlq_table, :size)
  end

  @impl true
  def retry_all_dlq(state) do
    dlq_ops =
      :ets.tab2list(state.dlq_table)
      |> Enum.map(fn {_id, op} -> %{op | status: :pending, error: nil} end)
      |> Enum.sort_by(& &1.order)

    count = length(dlq_ops)

    queue_records = Enum.map(dlq_ops, fn op -> {{op.order, op.id}, op} end)
    index_records = Enum.map(dlq_ops, fn op -> {op.id, op} end)
    :ets.insert(state.queue_table, queue_records)
    :ets.insert(state.index_table, index_records)
    :ets.delete_all_objects(state.dlq_table)

    {count, state}
  end

  @impl true
  def drain_dlq(state) do
    dlq_ids =
      :ets.tab2list(state.dlq_table)
      |> Enum.map(fn {id, _op} -> id end)

    count = length(dlq_ids)
    Enum.each(dlq_ids, fn id -> :ets.delete(state.index_table, id) end)
    :ets.delete_all_objects(state.dlq_table)

    {count, state}
  end

  @impl true
  def queue_size(state) do
    :ets.info(state.queue_table, :size)
  end

  @impl true
  def completed_size(state) do
    :ets.info(state.completed_table, :size)
  end

  @impl true
  def purge_completed(state, max_age_ms, max_count) do
    now = DateTime.utc_now()
    all = :ets.tab2list(state.completed_table)

    {expired, remaining} =
      Enum.split_with(all, fn {_key, op} ->
        case op.completed_at do
          nil -> false
          ts -> DateTime.diff(now, ts, :millisecond) > max_age_ms
        end
      end)

    Enum.each(expired, fn {key, op} ->
      :ets.delete(state.completed_table, key)
      :ets.delete(state.index_table, op.id)
    end)

    over_count =
      if length(remaining) > max_count do
        remaining
        |> Enum.sort_by(fn {_key, op} -> op.completed_at end, {:asc, DateTime})
        |> Enum.take(length(remaining) - max_count)
      else
        []
      end

    Enum.each(over_count, fn {key, op} ->
      :ets.delete(state.completed_table, key)
      :ets.delete(state.index_table, op.id)
    end)

    purged = length(expired) + length(over_count)
    {purged, state}
  end

  @impl true
  def clear_queue(state, error) do
    ops =
      :ets.tab2list(state.queue_table)
      |> Enum.map(fn {_key, op} -> op end)

    count = length(ops)

    Enum.each(ops, fn op ->
      updated = %{op | status: :dead, error: error}
      :ets.insert(state.dlq_table, {op.id, updated})
      :ets.insert(state.index_table, {op.id, updated})
    end)

    :ets.delete_all_objects(state.queue_table)

    {count, state}
  end

  # Public helpers for heir handoff

  @doc false
  def table_name(ferry_name, type) do
    :"ferry_#{ferry_name}_#{type}"
  end

  @doc false
  def set_heir(ferry_name, heir_pid) do
    for type <- [:queue, :dlq, :completed, :index] do
      table = table_name(ferry_name, type)

      if table_exists?(table) do
        :ets.setopts(table, {:heir, heir_pid, {ferry_name, type}})
      end
    end

    :ok
  end

  @doc false
  def give_tables(ferry_name, to_pid) do
    for type <- [:queue, :dlq, :completed, :index] do
      table = table_name(ferry_name, type)

      if table_exists?(table) do
        :ets.give_away(table, to_pid, {ferry_name, type})
      end
    end

    :ok
  end

  # Private helpers

  defp ensure_table(name, type) do
    if table_exists?(name) do
      name
    else
      :ets.new(name, [type, :public, :named_table])
    end
  end

  defp table_exists?(name) do
    :ets.whereis(name) != :undefined
  end

  defp take_first_n(table, n) do
    take_first_n(table, :ets.first(table), n, [])
  end

  defp take_first_n(_table, :"$end_of_table", _n, acc), do: Enum.reverse(acc)
  defp take_first_n(_table, _key, 0, acc), do: Enum.reverse(acc)

  defp take_first_n(table, key, n, acc) do
    case :ets.lookup(table, key) do
      [{_key, op}] ->
        take_first_n(table, :ets.next(table, key), n - 1, [op | acc])

      [] ->
        take_first_n(table, :ets.next(table, key), n, acc)
    end
  end

  defp delete_from_queue_by_id(table, id, order) do
    :ets.delete(table, {order, id})
  end
end
