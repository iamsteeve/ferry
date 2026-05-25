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

  describe "drain_completed/1" do
    test "wipes completed history" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      assert {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(name, id)

      assert {:ok, 1} = Ferry.drain_completed(name)
      assert {:error, :not_found} = Ferry.status(name, id)
      assert {:ok, 0} = Ferry.drain_completed(name)
    end
  end

  describe "delete/2" do
    test "removes a pending operation" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :payload)

      assert :ok = Ferry.delete(name, id)
      assert Ferry.queue_size(name) == 0
      assert {:error, :not_found} = Ferry.status(name, id)
    end

    test "removes a completed operation" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      assert :ok = Ferry.delete(name, id)
      assert {:error, :not_found} = Ferry.status(name, id)
    end

    test "returns not_found for unknown ID" do
      name = start_ferry()
      assert {:error, :not_found} = Ferry.delete(name, "fry_nope")
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

  describe "hibernation" do
    test "process hibernates after draining the queue via flush" do
      name = start_ferry()
      {:ok, _ids} = Ferry.push_many(name, Enum.to_list(1..10))
      :ok = Ferry.flush(name)

      pid = Process.whereis(:"#{name}.Server")
      assert is_pid(pid)

      wait_until(fn -> hibernated?(pid) end, 500)
      assert hibernated?(pid)
    end

    test "process does not hibernate while a flush is still in flight" do
      # slow_resolver keeps `flushing != nil` long enough for us to observe.
      name = start_ferry(resolver: slow_resolver(150))
      {:ok, _id} = Ferry.push(name, :a)

      flush_task = Task.async(fn -> Ferry.flush(name) end)

      pid = Process.whereis(:"#{name}.Server")
      assert is_pid(pid)

      # While the resolver is sleeping, we should not be hibernated.
      Process.sleep(50)
      refute hibernated?(pid)

      :ok = Task.await(flush_task, 1_000)
      wait_until(fn -> hibernated?(pid) end, 500)
      assert hibernated?(pid)
    end

    test "heap shrinks after hibernation" do
      name = start_ferry(max_queue_size: 1_000, batch_size: 200)
      pid = Process.whereis(:"#{name}.Server")

      # Inflate the heap with a large push burst.
      payloads = Enum.map(1..200, fn i -> %{data: String.duplicate("x", 500), i: i} end)
      {:ok, _ids} = Ferry.push_many(name, payloads)

      {:heap_size, peak_heap} = Process.info(pid, :heap_size)

      :ok = Ferry.flush(name)
      {:ok, _count} = Ferry.drain_completed(name)

      wait_until(fn -> hibernated?(pid) end, 500)
      {:heap_size, hibernated_heap} = Process.info(pid, :heap_size)

      # After hibernate, the heap should be substantially smaller than the peak.
      assert hibernated_heap < peak_heap
    end
  end

  defp hibernated?(pid) do
    case Process.info(pid, :current_function) do
      {:current_function, {:erlang, :hibernate, _}} -> true
      {:current_function, {:gen_server, :loop_hibernate, _}} -> true
      _ -> false
    end
  end

  defp wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        :timeout
      else
        Process.sleep(10)
        do_wait_until(fun, deadline)
      end
    end
  end
end
