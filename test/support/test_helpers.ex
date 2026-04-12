defmodule Ferry.TestHelpers do
  @moduledoc false

  def success_resolver do
    fn %Ferry.Batch{operations: ops} ->
      Enum.map(ops, fn op -> {op.id, :ok, {:processed, op.payload}} end)
    end
  end

  def failure_resolver do
    fn %Ferry.Batch{operations: ops} ->
      Enum.map(ops, fn op -> {op.id, :error, :forced_failure} end)
    end
  end

  def partial_resolver do
    fn %Ferry.Batch{operations: ops} ->
      # Only return results for the first half
      ops
      |> Enum.take(div(length(ops), 2))
      |> Enum.map(fn op -> {op.id, :ok, :partial} end)
    end
  end

  def crash_resolver do
    fn _batch ->
      raise "boom"
    end
  end

  def slow_resolver(delay_ms) do
    fn %Ferry.Batch{operations: ops} ->
      Process.sleep(delay_ms)
      Enum.map(ops, fn op -> {op.id, :ok, :slow} end)
    end
  end

  def unique_name do
    :"ferry_test_#{System.unique_integer([:positive, :monotonic])}"
  end

  def start_ferry(opts \\ []) do
    defaults = [
      name: unique_name(),
      resolver: success_resolver(),
      auto_flush: false,
      batch_size: 10,
      max_queue_size: 100,
      flush_interval: 60_000
    ]

    merged = Keyword.merge(defaults, opts)
    {:ok, _pid} = Ferry.start_link(merged)
    merged[:name]
  end
end
