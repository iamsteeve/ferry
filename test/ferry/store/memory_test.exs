defmodule Ferry.Store.MemoryTest do
  use ExUnit.Case, async: true

  alias Ferry.Store.Memory
  alias Ferry.Operation

  setup do
    {:ok, state} = Memory.init(:test, [])
    %{state: state}
  end

  defp build_op(id, order, payload \\ :data) do
    %Operation{
      id: id,
      payload: payload,
      order: order,
      status: :pending,
      pushed_at: DateTime.utc_now()
    }
  end

  describe "push/2" do
    test "adds operation to queue", %{state: state} do
      op = build_op("op1", 1)
      {:ok, state} = Memory.push(state, op)
      assert Memory.queue_size(state) == 1
    end
  end

  describe "push_many/2" do
    test "adds multiple operations", %{state: state} do
      ops = [build_op("op1", 1), build_op("op2", 2)]
      {:ok, state} = Memory.push_many(state, ops)
      assert Memory.queue_size(state) == 2
    end
  end

  describe "pop_batch/2" do
    test "returns operations in FIFO order", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {:ok, state} = Memory.push(state, build_op("op2", 2))
      {:ok, state} = Memory.push(state, build_op("op3", 3))

      {ops, _state} = Memory.pop_batch(state, 2)
      assert length(ops) == 2
      assert Enum.map(ops, & &1.id) == ["op1", "op2"]
      assert Enum.all?(ops, &(&1.status == :processing))
    end

    test "returns empty list when queue is empty", %{state: state} do
      {ops, _state} = Memory.pop_batch(state, 5)
      assert ops == []
    end
  end

  describe "get/2" do
    test "finds operation by ID", %{state: state} do
      op = build_op("findme", 1)
      {:ok, state} = Memory.push(state, op)
      assert {:ok, %Operation{id: "findme"}} = Memory.get(state, "findme")
    end

    test "returns error for unknown ID", %{state: state} do
      assert {:error, :not_found} = Memory.get(state, "nope")
    end
  end

  describe "mark_completed/4" do
    test "moves operation to completed", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)

      now = DateTime.utc_now()
      {:ok, state} = Memory.mark_completed(state, "op1", :result, now, nil)

      assert {:ok, op} = Memory.get(state, "op1")
      assert op.status == :completed
      assert op.result == :result
      assert Memory.completed_size(state) == 1
    end
  end

  describe "mark_failed/4" do
    test "moves operation to DLQ", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)

      now = DateTime.utc_now()
      {:ok, state} = Memory.mark_failed(state, "op1", :bad, now, nil)

      assert {:ok, op} = Memory.get(state, "op1")
      assert op.status == :dead
      assert Memory.dlq_size(state) == 1
    end
  end

  describe "DLQ operations" do
    test "retry_all_dlq moves ops back to queue", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.move_to_dlq(state, "op1", :err)

      assert Memory.dlq_size(state) == 1
      assert Memory.queue_size(state) == 0

      {count, state} = Memory.retry_all_dlq(state)
      assert count == 1
      assert Memory.dlq_size(state) == 0
      assert Memory.queue_size(state) == 1
    end

    test "drain_dlq permanently removes ops", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.move_to_dlq(state, "op1", :err)

      {count, state} = Memory.drain_dlq(state)
      assert count == 1
      assert Memory.dlq_size(state) == 0
      assert {:error, :not_found} = Memory.get(state, "op1")
    end
  end

  describe "memory_bytes/1" do
    test "returns positive bytes for empty state", %{state: state} do
      bytes = Memory.memory_bytes(state)
      assert is_integer(bytes)
      assert bytes > 0
    end

    test "increases after pushing operations", %{state: state} do
      before = Memory.memory_bytes(state)

      ops = Enum.map(1..50, &build_op("op#{&1}", &1, String.duplicate("x", 100)))
      {:ok, state} = Memory.push_many(state, ops)

      after_push = Memory.memory_bytes(state)
      assert after_push > before
    end
  end

  describe "drain_completed/1" do
    test "removes all completed operations", %{state: state} do
      now = DateTime.utc_now()

      state =
        Enum.reduce(1..3, state, fn i, acc ->
          {:ok, acc} = Memory.push(acc, build_op("op#{i}", i))
          {_ops, acc} = Memory.pop_batch(acc, 1)
          {:ok, acc} = Memory.mark_completed(acc, "op#{i}", :ok, now, nil)
          acc
        end)

      assert Memory.completed_size(state) == 3
      {count, state} = Memory.drain_completed(state)
      assert count == 3
      assert Memory.completed_size(state) == 0
      assert {:error, :not_found} = Memory.get(state, "op1")
    end

    test "returns 0 when no completed operations", %{state: state} do
      {count, _state} = Memory.drain_completed(state)
      assert count == 0
    end
  end

  describe "delete/2" do
    test "removes a pending operation", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {:ok, state} = Memory.push(state, build_op("op2", 2))

      {:ok, state} = Memory.delete(state, "op1")

      assert Memory.queue_size(state) == 1
      assert {:error, :not_found} = Memory.get(state, "op1")
      assert {:ok, _} = Memory.get(state, "op2")
    end

    test "removes a completed operation", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.mark_completed(state, "op1", :ok, DateTime.utc_now(), nil)

      assert Memory.completed_size(state) == 1
      {:ok, state} = Memory.delete(state, "op1")

      assert Memory.completed_size(state) == 0
      assert {:error, :not_found} = Memory.get(state, "op1")
    end

    test "removes a dead-lettered operation", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.move_to_dlq(state, "op1", :err)

      assert Memory.dlq_size(state) == 1
      {:ok, state} = Memory.delete(state, "op1")

      assert Memory.dlq_size(state) == 0
      assert {:error, :not_found} = Memory.get(state, "op1")
    end

    test "returns not_found for unknown ID", %{state: state} do
      {result, _state} = Memory.delete(state, "nope")
      assert result == {:error, :not_found}
    end
  end

  describe "lite index" do
    test "index entry for a completed op has no payload/result/error", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1, %{big: String.duplicate("x", 200)}))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.mark_completed(state, "op1", :result_value, DateTime.utc_now(), "b1")

      lite = Map.fetch!(state.index, "op1")
      assert lite.status == :completed
      assert lite.payload == nil
      assert lite.result == nil
      assert lite.error == nil
      # Bookkeeping fields needed for delete/2 and queries are preserved.
      assert lite.id == "op1"
      assert lite.order == 1
      assert lite.batch_id == "b1"
      assert %DateTime{} = lite.completed_at
    end

    test "index entry for a dead op has no payload/result/error", %{state: state} do
      {:ok, state} = Memory.push(state, build_op("op1", 1, %{big: String.duplicate("x", 200)}))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.mark_failed(state, "op1", :err, DateTime.utc_now(), nil)

      lite = Map.fetch!(state.index, "op1")
      assert lite.status == :dead
      assert lite.payload == nil
      assert lite.error == nil
    end

    test "get/2 hydrates a completed op back to the full record", %{state: state} do
      payload = %{key: "value", n: 42}
      {:ok, state} = Memory.push(state, build_op("op1", 1, payload))
      {_ops, state} = Memory.pop_batch(state, 1)
      {:ok, state} = Memory.mark_completed(state, "op1", :result_value, DateTime.utc_now(), "b1")

      {:ok, op} = Memory.get(state, "op1")
      assert op.status == :completed
      assert op.payload == payload
      assert op.result == :result_value
      assert op.batch_id == "b1"
    end

    test "get/2 hydrates a dead op back to the full record", %{state: state} do
      payload = %{key: "value"}
      {:ok, state} = Memory.push(state, build_op("op1", 1, payload))
      {_ops, state} = Memory.pop_batch(state, 1)

      {:ok, state} =
        Memory.mark_failed(state, "op1", {:fatal, :badness}, DateTime.utc_now(), nil)

      {:ok, op} = Memory.get(state, "op1")
      assert op.status == :dead
      assert op.payload == payload
      assert op.error == {:fatal, :badness}
    end

    test "memory_bytes is lower with lite index vs hypothetical full duplication",
         %{state: state} do
      # Push many ops with a sizable payload, mark them all completed, then
      # measure. The duplicated bytes would dominate without the lite index.
      payload = %{blob: String.duplicate("x", 500)}

      state =
        Enum.reduce(1..100, state, fn i, acc ->
          {:ok, acc} = Memory.push(acc, build_op("op#{i}", i, payload))
          {_ops, acc} = Memory.pop_batch(acc, 1)
          {:ok, acc} = Memory.mark_completed(acc, "op#{i}", :ok, DateTime.utc_now(), nil)
          acc
        end)

      lite_bytes = Memory.memory_bytes(state)

      # Synthetically inflate the index back to full ops to estimate the
      # baseline. This gives us a concrete delta to assert on.
      inflated_index =
        Map.new(state.index, fn {id, _lite} -> {id, Map.fetch!(state.completed, id)} end)

      full_bytes = :erlang.external_size(%{state | index: inflated_index})
      assert lite_bytes < full_bytes
      # Sanity check: savings should be meaningful, not just a few bytes.
      assert full_bytes - lite_bytes > 10_000
    end
  end

  describe "purge_completed/3" do
    test "purges by max count", %{state: state} do
      now = DateTime.utc_now()

      state =
        Enum.reduce(1..5, state, fn i, acc ->
          {:ok, acc} = Memory.push(acc, build_op("op#{i}", i))
          {_ops, acc} = Memory.pop_batch(acc, 1)
          {:ok, acc} = Memory.mark_completed(acc, "op#{i}", :ok, now, nil)
          acc
        end)

      assert Memory.completed_size(state) == 5
      {purged, state} = Memory.purge_completed(state, :timer.hours(1), 3)
      assert purged == 2
      assert Memory.completed_size(state) == 3
    end
  end
end
