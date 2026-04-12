defmodule Ferry.Store.EtsTest do
  use ExUnit.Case

  alias Ferry.Store.Ets
  alias Ferry.Operation

  setup do
    name = :"ets_test_#{System.unique_integer([:positive, :monotonic])}"
    {:ok, state} = Ets.init(name, [])

    on_exit(fn ->
      for type <- [:queue, :dlq, :completed, :index] do
        table = Ets.table_name(name, type)

        if :ets.whereis(table) != :undefined do
          :ets.delete(table)
        end
      end
    end)

    %{state: state, name: name}
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

  describe "push and pop" do
    test "push adds to queue and pop retrieves in order", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {:ok, state} = Ets.push(state, build_op("op2", 2))

      assert Ets.queue_size(state) == 2

      {ops, state} = Ets.pop_batch(state, 2)
      assert length(ops) == 2
      assert Enum.map(ops, & &1.id) == ["op1", "op2"]
      assert Ets.queue_size(state) == 0
    end
  end

  describe "push_many" do
    test "inserts multiple operations", %{state: state} do
      ops = [build_op("op1", 1), build_op("op2", 2), build_op("op3", 3)]
      {:ok, state} = Ets.push_many(state, ops)
      assert Ets.queue_size(state) == 3
    end
  end

  describe "get" do
    test "finds operation in queue", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1, :payload))
      assert {:ok, %Operation{id: "op1", payload: :payload}} = Ets.get(state, "op1")
    end

    test "finds operation in completed", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {_ops, state} = Ets.pop_batch(state, 1)
      {:ok, state} = Ets.mark_completed(state, "op1", :result, DateTime.utc_now(), nil)
      assert {:ok, %Operation{status: :completed}} = Ets.get(state, "op1")
    end

    test "finds operation in DLQ", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {_ops, state} = Ets.pop_batch(state, 1)
      {:ok, state} = Ets.move_to_dlq(state, "op1", :err)
      assert {:ok, %Operation{status: :dead}} = Ets.get(state, "op1")
    end

    test "returns error for unknown ID", %{state: state} do
      assert {:error, :not_found} = Ets.get(state, "nope")
    end
  end

  describe "DLQ operations" do
    test "retry_all_dlq moves ops back", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {_ops, state} = Ets.pop_batch(state, 1)
      {:ok, state} = Ets.move_to_dlq(state, "op1", :err)

      {count, state} = Ets.retry_all_dlq(state)
      assert count == 1
      assert Ets.dlq_size(state) == 0
      assert Ets.queue_size(state) == 1
    end

    test "drain_dlq removes all", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {_ops, state} = Ets.pop_batch(state, 1)
      {:ok, state} = Ets.move_to_dlq(state, "op1", :err)

      {count, _state} = Ets.drain_dlq(state)
      assert count == 1
    end
  end

  describe "completed management" do
    test "mark_completed and list", %{state: state} do
      {:ok, state} = Ets.push(state, build_op("op1", 1))
      {_ops, state} = Ets.pop_batch(state, 1)
      {:ok, state} = Ets.mark_completed(state, "op1", :done, DateTime.utc_now(), nil)

      assert Ets.completed_size(state) == 1
      completed = Ets.list_completed(state, limit: 10)
      assert length(completed) == 1
      assert hd(completed).result == :done
    end
  end
end
