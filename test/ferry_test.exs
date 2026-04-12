defmodule FerryTest do
  use ExUnit.Case, async: true

  import Ferry.TestHelpers

  describe "push/2" do
    test "returns {:ok, id} with a prefixed ID" do
      name = start_ferry()
      assert {:ok, "fry_" <> _rest} = Ferry.push(name, %{data: 1})
    end

    test "returns unique IDs for each push" do
      name = start_ferry()
      {:ok, id1} = Ferry.push(name, :a)
      {:ok, id2} = Ferry.push(name, :b)
      assert id1 != id2
    end

    test "increments queue size" do
      name = start_ferry()
      assert Ferry.queue_size(name) == 0
      {:ok, _} = Ferry.push(name, :a)
      assert Ferry.queue_size(name) == 1
      {:ok, _} = Ferry.push(name, :b)
      assert Ferry.queue_size(name) == 2
    end

    test "returns {:error, :queue_full} when max_queue_size is reached" do
      name = start_ferry(max_queue_size: 2)
      {:ok, _} = Ferry.push(name, :a)
      {:ok, _} = Ferry.push(name, :b)
      assert {:error, :queue_full} = Ferry.push(name, :c)
    end
  end

  describe "push_many/2" do
    test "pushes multiple operations atomically" do
      name = start_ferry()
      assert {:ok, ids} = Ferry.push_many(name, [:a, :b, :c])
      assert length(ids) == 3
      assert Ferry.queue_size(name) == 3
    end

    test "rejects all if queue can't fit all" do
      name = start_ferry(max_queue_size: 2)
      assert {:error, :queue_full} = Ferry.push_many(name, [:a, :b, :c])
      assert Ferry.queue_size(name) == 0
    end

    test "succeeds when exactly at capacity" do
      name = start_ferry(max_queue_size: 3)
      assert {:ok, _} = Ferry.push_many(name, [:a, :b, :c])
      assert Ferry.queue_size(name) == 3
    end
  end

  describe "flush/1" do
    test "processes batch and completes operations" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, %{value: 42})
      assert :ok = Ferry.flush(name)

      assert {:ok, %Ferry.Operation{status: :completed, result: {:processed, %{value: 42}}}} =
               Ferry.status(name, id)
    end

    test "returns {:error, :empty_queue} when queue is empty" do
      name = start_ferry()
      assert {:error, :empty_queue} = Ferry.flush(name)
    end

    test "processes at most batch_size operations" do
      name = start_ferry(batch_size: 2)

      {:ok, _ids} = Ferry.push_many(name, [:a, :b, :c, :d])
      assert :ok = Ferry.flush(name)

      # 2 processed, 2 remaining
      assert Ferry.queue_size(name) == 2
    end

    test "processes operations in FIFO order" do
      name = start_ferry(batch_size: 3)

      {:ok, [id1, id2, id3]} = Ferry.push_many(name, [:first, :second, :third])
      :ok = Ferry.flush(name)

      {:ok, op1} = Ferry.status(name, id1)
      {:ok, op2} = Ferry.status(name, id2)
      {:ok, op3} = Ferry.status(name, id3)

      assert op1.status == :completed
      assert op2.status == :completed
      assert op3.status == :completed
      assert op1.order < op2.order
      assert op2.order < op3.order
    end
  end

  describe "status/2" do
    test "returns pending operation" do
      name = start_ferry()
      {:ok, id} = Ferry.push(name, :payload)
      assert {:ok, %Ferry.Operation{status: :pending, payload: :payload}} = Ferry.status(name, id)
    end

    test "returns {:error, :not_found} for unknown ID" do
      name = start_ferry()
      assert {:error, :not_found} = Ferry.status(name, "nonexistent")
    end
  end

  describe "resolver failure handling" do
    test "failed operations go to DLQ" do
      name = start_ferry(resolver: failure_resolver())
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      assert {:ok, %Ferry.Operation{status: :dead, error: :forced_failure}} =
               Ferry.status(name, id)

      assert Ferry.dead_letter_count(name) == 1
    end

    test "missing results mark operations as failed" do
      name = start_ferry(resolver: partial_resolver(), batch_size: 4)

      {:ok, ids} = Ferry.push_many(name, [:a, :b, :c, :d])
      :ok = Ferry.flush(name)

      statuses =
        Enum.map(ids, fn id ->
          {:ok, op} = Ferry.status(name, id)
          op.status
        end)

      # First 2 completed (partial resolver returns first half), last 2 dead
      assert Enum.count(statuses, &(&1 == :completed)) == 2
      assert Enum.count(statuses, &(&1 == :dead)) == 2
    end

    test "resolver crash moves entire batch to DLQ" do
      name = start_ferry(resolver: crash_resolver())
      {:ok, [id1, id2]} = Ferry.push_many(name, [:a, :b])
      :ok = Ferry.flush(name)

      assert {:ok, %Ferry.Operation{status: :dead}} = Ferry.status(name, id1)
      assert {:ok, %Ferry.Operation{status: :dead}} = Ferry.status(name, id2)
      assert Ferry.dead_letter_count(name) == 2
    end

    test "resolver timeout moves entire batch to DLQ" do
      name = start_ferry(resolver: slow_resolver(500), operation_timeout: 100)
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      assert {:ok, %Ferry.Operation{status: :dead, error: :timeout}} = Ferry.status(name, id)
    end
  end

  describe "DLQ operations" do
    test "dead_letters/1 lists dead-lettered operations" do
      name = start_ferry(resolver: failure_resolver())
      {:ok, _} = Ferry.push(name, :a)
      {:ok, _} = Ferry.push(name, :b)
      :ok = Ferry.flush(name)

      dead = Ferry.dead_letters(name)
      assert length(dead) == 2
      assert Enum.all?(dead, &(&1.status == :dead))
    end

    test "retry_dead_letters/1 moves operations back to queue" do
      name = start_ferry(resolver: failure_resolver())
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      assert Ferry.dead_letter_count(name) == 1
      assert Ferry.queue_size(name) == 0

      assert {:ok, 1} = Ferry.retry_dead_letters(name)
      assert Ferry.dead_letter_count(name) == 0
      assert Ferry.queue_size(name) == 1
    end

    test "drain_dead_letters/1 permanently discards dead letters" do
      name = start_ferry(resolver: failure_resolver())
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      assert :ok = Ferry.drain_dead_letters(name)
      assert Ferry.dead_letter_count(name) == 0
    end
  end

  describe "pause/resume" do
    test "pause stops auto-flush, resume restarts it" do
      name = start_ferry(auto_flush: true, flush_interval: 100)
      :ok = Ferry.pause(name)

      {:ok, _} = Ferry.push(name, :payload)
      Process.sleep(200)

      # Should still be in queue since paused
      assert Ferry.queue_size(name) == 1

      :ok = Ferry.resume(name)
      Process.sleep(200)

      # Should have been flushed
      assert Ferry.queue_size(name) == 0
    end

    test "manual flush works while paused" do
      name = start_ferry()
      :ok = Ferry.pause(name)
      {:ok, _} = Ferry.push(name, :payload)
      assert :ok = Ferry.flush(name)
      assert Ferry.queue_size(name) == 0
    end
  end

  describe "auto_flush" do
    test "auto-flush processes operations on interval" do
      name = start_ferry(auto_flush: true, flush_interval: 100)
      {:ok, id} = Ferry.push(name, :payload)
      Process.sleep(300)

      assert {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(name, id)
    end
  end

  describe "stats/1" do
    test "returns current statistics" do
      name = start_ferry()
      {:ok, _} = Ferry.push(name, :a)
      {:ok, _} = Ferry.push(name, :b)
      :ok = Ferry.flush(name)

      stats = Ferry.stats(name)
      assert %Ferry.Stats{} = stats
      assert stats.total_pushed == 2
      assert stats.total_processed == 2
      assert stats.queue_size == 0
      assert stats.batches_executed == 1
      assert stats.status == :running
      assert stats.uptime_ms >= 0
    end

    test "tracks rejected operations" do
      name = start_ferry(max_queue_size: 1)
      {:ok, _} = Ferry.push(name, :a)
      {:error, :queue_full} = Ferry.push(name, :b)

      stats = Ferry.stats(name)
      assert stats.total_rejected == 1
    end
  end

  describe "pending/2 and completed/2" do
    test "pending lists queued operations" do
      name = start_ferry()
      {:ok, _} = Ferry.push_many(name, [:a, :b, :c])

      pending = Ferry.pending(name, limit: 2)
      assert length(pending) == 2
      assert Enum.all?(pending, &(&1.status == :pending))
    end

    test "completed lists processed operations" do
      name = start_ferry()
      {:ok, _} = Ferry.push_many(name, [:a, :b])
      :ok = Ferry.flush(name)

      completed = Ferry.completed(name, limit: 10)
      assert length(completed) == 2
      assert Enum.all?(completed, &(&1.status == :completed))
    end
  end

  describe "status_many/2" do
    test "returns statuses for multiple operations" do
      name = start_ferry()
      {:ok, id1} = Ferry.push(name, :a)
      {:ok, id2} = Ferry.push(name, :b)

      results = Ferry.status_many(name, [id1, id2])

      assert {:ok, %Ferry.Operation{status: :pending, payload: :a}} = results[id1]
      assert {:ok, %Ferry.Operation{status: :pending, payload: :b}} = results[id2]
    end

    test "returns :not_found for unknown IDs" do
      name = start_ferry()
      {:ok, id1} = Ferry.push(name, :a)

      results = Ferry.status_many(name, [id1, "nonexistent"])

      assert {:ok, %Ferry.Operation{}} = results[id1]
      assert {:error, :not_found} = results["nonexistent"]
    end

    test "works with completed and dead operations" do
      name = start_ferry()
      {:ok, id1} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      name2 = start_ferry(resolver: failure_resolver())
      {:ok, id2} = Ferry.push(name2, :b)
      :ok = Ferry.flush(name2)

      results1 = Ferry.status_many(name, [id1])
      assert {:ok, %Ferry.Operation{status: :completed}} = results1[id1]

      results2 = Ferry.status_many(name2, [id2])
      assert {:ok, %Ferry.Operation{status: :dead}} = results2[id2]
    end
  end

  describe "clear/1" do
    test "clears all pending operations" do
      name = start_ferry()
      {:ok, _} = Ferry.push_many(name, [:a, :b, :c])
      assert Ferry.queue_size(name) == 3

      assert {:ok, 3} = Ferry.clear(name)
      assert Ferry.queue_size(name) == 0
    end

    test "cleared operations go to DLQ with :canceled error" do
      name = start_ferry()
      {:ok, [id1, id2]} = Ferry.push_many(name, [:a, :b])

      {:ok, 2} = Ferry.clear(name)

      assert Ferry.dead_letter_count(name) == 2
      assert {:ok, %Ferry.Operation{status: :dead, error: :canceled}} = Ferry.status(name, id1)
      assert {:ok, %Ferry.Operation{status: :dead, error: :canceled}} = Ferry.status(name, id2)
    end

    test "returns {:ok, 0} when queue is empty" do
      name = start_ferry()
      assert {:ok, 0} = Ferry.clear(name)
    end

    test "does not affect already completed operations" do
      name = start_ferry()
      {:ok, id1} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      {:ok, _id2} = Ferry.push(name, :b)
      {:ok, 1} = Ferry.clear(name)

      assert {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(name, id1)
      assert Ferry.queue_size(name) == 0
    end

    test "cleared operations can be retried via DLQ" do
      name = start_ferry()
      {:ok, _} = Ferry.push_many(name, [:a, :b])
      {:ok, 2} = Ferry.clear(name)

      assert {:ok, 2} = Ferry.retry_dead_letters(name)
      assert Ferry.queue_size(name) == 2
      assert Ferry.dead_letter_count(name) == 0
    end
  end

  describe "multiple independent instances" do
    test "instances don't interfere with each other" do
      name1 = start_ferry(resolver: success_resolver())
      name2 = start_ferry(resolver: failure_resolver())

      {:ok, id1} = Ferry.push(name1, :a)
      {:ok, id2} = Ferry.push(name2, :b)

      :ok = Ferry.flush(name1)
      :ok = Ferry.flush(name2)

      assert {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(name1, id1)
      assert {:ok, %Ferry.Operation{status: :dead}} = Ferry.status(name2, id2)

      assert Ferry.stats(name1).total_processed == 1
      assert Ferry.stats(name2).total_failed == 1
    end
  end

  describe "use Ferry macro" do
    defmodule TestResolver do
      def resolve(%Ferry.Batch{operations: ops}) do
        Enum.map(ops, fn op -> {op.id, :ok, {:processed, op.payload}} end)
      end
    end

    defmodule TestFerry do
      use Ferry,
        resolver: &FerryTest.TestResolver.resolve/1,
        auto_flush: false,
        batch_size: 5
    end

    test "generates child_spec/1" do
      spec = TestFerry.child_spec([])
      assert spec.id == TestFerry
      assert spec.type == :supervisor
    end
  end

  describe "custom id_generator" do
    test "uses custom generator" do
      counter = :counters.new(1, [:atomics])

      generator = fn _payload ->
        :counters.add(counter, 1, 1)
        "custom_#{:counters.get(counter, 1)}"
      end

      name = start_ferry(id_generator: generator)
      {:ok, id1} = Ferry.push(name, :a)
      {:ok, id2} = Ferry.push(name, :b)

      assert id1 == "custom_1"
      assert id2 == "custom_2"
    end
  end
end
