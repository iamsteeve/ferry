defmodule Ferry.ServerTest do
  use ExUnit.Case, async: true

  import Ferry.TestHelpers

  describe "init" do
    test "starts with empty queue" do
      name = start_ferry()
      assert Ferry.queue_size(name) == 0
    end

    test "requires name option" do
      assert_raise KeyError, fn ->
        Ferry.start_link(resolver: success_resolver())
      end
    end

    test "requires resolver option" do
      Process.flag(:trap_exit, true)
      result = Ferry.start_link(name: unique_name())
      assert {:error, _} = result
    end
  end

  describe "back-pressure" do
    test "rejects at exact max_queue_size" do
      name = start_ferry(max_queue_size: 3)
      {:ok, _} = Ferry.push(name, :a)
      {:ok, _} = Ferry.push(name, :b)
      {:ok, _} = Ferry.push(name, :c)
      assert {:error, :queue_full} = Ferry.push(name, :d)
    end

    test "push_many checks total space needed" do
      name = start_ferry(max_queue_size: 5)
      {:ok, _} = Ferry.push_many(name, [:a, :b, :c])
      # 3 in queue, trying to add 3 more (total 6 > 5)
      assert {:error, :queue_full} = Ferry.push_many(name, [:d, :e, :f])
      assert Ferry.queue_size(name) == 3
    end
  end

  describe "completed history" do
    test "completed operations are queryable" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      {:ok, op} = Ferry.status(name, id)
      assert op.status == :completed
      assert op.completed_at != nil
      assert op.result == {:processed, :payload}
    end
  end

  describe "operation lifecycle" do
    test "operation has all fields populated" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, %{key: "value"})

      {:ok, op} = Ferry.status(name, id)
      assert op.id == id
      assert op.payload == %{key: "value"}
      assert op.order == 0
      assert op.status == :pending
      assert %DateTime{} = op.pushed_at
      assert op.completed_at == nil
      assert op.result == nil
      assert op.error == nil
    end

    test "completed operation has result and timestamp" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :data)
      :ok = Ferry.flush(name)

      {:ok, op} = Ferry.status(name, id)
      assert op.status == :completed
      assert op.result == {:processed, :data}
      assert %DateTime{} = op.completed_at
    end
  end

  describe "stats/1" do
    test "includes memory_bytes" do
      name = start_ferry()
      stats = Ferry.stats(name)
      assert is_integer(stats.memory_bytes)
      assert stats.memory_bytes > 0
    end

    test "memory_bytes grows with operations" do
      name = start_ferry()
      stats_before = Ferry.stats(name)

      Enum.each(1..50, fn i ->
        Ferry.push(name, %{data: String.duplicate("payload", 10), index: i})
      end)

      stats_after = Ferry.stats(name)
      assert stats_after.memory_bytes > stats_before.memory_bytes
    end
  end
end
