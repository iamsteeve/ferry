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
