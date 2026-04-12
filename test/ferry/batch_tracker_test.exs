defmodule Ferry.BatchTrackerTest do
  use ExUnit.Case, async: true

  import Ferry.TestHelpers

  defp await_history(name, expected_count, retries \\ 30) do
    history = Ferry.batch_history(name)

    if length(history) >= expected_count or retries <= 0 do
      history
    else
      Process.sleep(10)
      await_history(name, expected_count, retries - 1)
    end
  end

  describe "disabled by default" do
    test "batch_history returns :not_enabled" do
      name = start_ferry()
      assert {:error, :not_enabled} = Ferry.batch_history(name)
    end

    test "batch_info returns :not_enabled" do
      name = start_ferry()
      assert {:error, :not_enabled} = Ferry.batch_info(name, "any")
    end

    test "purge_batch_history returns :not_enabled" do
      name = start_ferry()
      assert {:error, :not_enabled} = Ferry.purge_batch_history(name)
    end
  end

  describe "batch tracking enabled" do
    test "records batch on flush" do
      name = start_ferry(batch_tracking: true)
      {:ok, [id1, id2]} = Ferry.push_many(name, [:a, :b])
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)
      assert record.status == :completed
      assert record.size == 2
      assert record.trigger == :manual
      assert record.succeeded == 2
      assert record.failed == 0
      assert record.duration_ms >= 0
      assert record.completed_at != nil
      assert id1 in record.operation_ids
      assert id2 in record.operation_ids
    end

    test "operation has batch_id after flush" do
      name = start_ferry(batch_tracking: true)
      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)

      {:ok, op} = Ferry.status(name, id)
      assert op.batch_id != nil
      assert op.batch_id == record.id
    end

    test "batch_info returns specific batch" do
      name = start_ferry(batch_tracking: true)
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)
      assert {:ok, info} = Ferry.batch_info(name, record.id)
      assert info.id == record.id
      assert info.succeeded == 1
    end

    test "batch_info returns :not_found for unknown batch" do
      name = start_ferry(batch_tracking: true)
      assert {:error, :not_found} = Ferry.batch_info(name, "nonexistent")
    end

    test "multiple batches tracked newest first" do
      name = start_ferry(batch_tracking: true, batch_size: 1)
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)
      {:ok, _} = Ferry.push(name, :b)
      :ok = Ferry.flush(name)

      history = await_history(name, 2)
      assert length(history) == 2

      [newest, oldest] = history
      assert newest.started_at >= oldest.started_at
    end

    test "tracks failed batches" do
      name = start_ferry(batch_tracking: true, resolver: failure_resolver())
      {:ok, _} = Ferry.push_many(name, [:a, :b])
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)
      assert record.status == :completed
      assert record.succeeded == 0
      assert record.failed == 2
    end

    test "tracks timeout batches" do
      name =
        start_ferry(batch_tracking: true, resolver: slow_resolver(500), operation_timeout: 100)

      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)
      assert record.status == :timeout
      assert record.failed == 1
    end

    test "purge removes all history" do
      name = start_ferry(batch_tracking: true)
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      assert length(await_history(name, 1)) == 1

      assert :ok = Ferry.purge_batch_history(name)
      assert Ferry.batch_history(name) == []
    end

    test "max_batch_history limits retained records" do
      name = start_ferry(batch_tracking: true, max_batch_history: 2, batch_size: 1)

      for _ <- 1..3 do
        {:ok, _} = Ferry.push(name, :x)
        :ok = Ferry.flush(name)
      end

      await_history(name, 2)
      history = Ferry.batch_history(name)
      assert length(history) == 2
    end

    test "limit option in batch_history" do
      name = start_ferry(batch_tracking: true, batch_size: 1)

      for _ <- 1..3 do
        {:ok, _} = Ferry.push(name, :x)
        :ok = Ferry.flush(name)
      end

      await_history(name, 3)

      assert length(Ferry.batch_history(name, limit: 1)) == 1
      assert length(Ferry.batch_history(name, limit: 10)) == 3
    end

    test "status filter in batch_history" do
      name = start_ferry(batch_tracking: true, batch_size: 1)
      {:ok, _} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      await_history(name, 1)

      assert length(Ferry.batch_history(name, status: :completed)) == 1
      assert Ferry.batch_history(name, status: :timeout) == []
    end

    test "batch tracker crash does not affect server" do
      name = start_ferry(batch_tracking: true)

      tracker_pid = Process.whereis(:"#{name}.BatchTracker")
      assert tracker_pid != nil
      Process.exit(tracker_pid, :kill)

      Process.sleep(100)

      {:ok, id} = Ferry.push(name, :payload)
      :ok = Ferry.flush(name)
      assert {:ok, %Ferry.Operation{status: :completed}} = Ferry.status(name, id)

      new_tracker_pid = Process.whereis(:"#{name}.BatchTracker")
      assert new_tracker_pid != nil
      assert new_tracker_pid != tracker_pid
    end

    test "dead-lettered operations carry batch_id" do
      name = start_ferry(batch_tracking: true, resolver: failure_resolver())
      {:ok, id} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      {:ok, op} = Ferry.status(name, id)
      assert op.status == :dead
      assert op.batch_id != nil
    end

    test "crashed resolver operations carry batch_id and batch is tracked as crashed" do
      name = start_ferry(batch_tracking: true, resolver: crash_resolver())
      {:ok, id} = Ferry.push(name, :a)
      :ok = Ferry.flush(name)

      [record] = await_history(name, 1)

      {:ok, op} = Ferry.status(name, id)
      assert op.status == :dead
      assert op.batch_id != nil
      assert record.status == :crashed
      assert record.failed == 1
    end
  end
end
