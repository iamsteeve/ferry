defmodule Ferry.TelemetryTest do
  use ExUnit.Case, async: true

  import Ferry.TestHelpers

  setup do
    test_pid = self()

    handler_id = "test_handler_#{System.unique_integer([:positive, :monotonic])}"

    events = [
      [:ferry, :operation, :pushed],
      [:ferry, :operation, :completed],
      [:ferry, :operation, :failed],
      [:ferry, :operation, :dead_lettered],
      [:ferry, :operation, :rejected],
      [:ferry, :batch, :started],
      [:ferry, :batch, :completed],
      [:ferry, :batch, :timeout],
      [:ferry, :queue, :paused],
      [:ferry, :queue, :resumed],
      [:ferry, :dlq, :retried],
      [:ferry, :dlq, :drained]
    ]

    :telemetry.attach_many(
      handler_id,
      events,
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  test "push emits [:ferry, :operation, :pushed]" do
    name = start_ferry()
    {:ok, id} = Ferry.push(name, :payload)

    assert_receive {:telemetry, [:ferry, :operation, :pushed], %{queue_size: 1},
                    %{ferry: ^name, operation_id: ^id}}
  end

  test "flush emits batch and operation events" do
    name = start_ferry()
    {:ok, id} = Ferry.push(name, :payload)
    :ok = Ferry.flush(name)

    assert_receive {:telemetry, [:ferry, :batch, :started], %{batch_size: 1}, %{ferry: ^name}}

    assert_receive {:telemetry, [:ferry, :operation, :completed], %{duration_ms: _},
                    %{ferry: ^name, operation_id: ^id}}

    assert_receive {:telemetry, [:ferry, :batch, :completed], %{succeeded: 1, failed: 0},
                    %{ferry: ^name}}
  end

  test "failed operations emit failure events" do
    name = start_ferry(resolver: failure_resolver())
    {:ok, id} = Ferry.push(name, :payload)
    :ok = Ferry.flush(name)

    assert_receive {:telemetry, [:ferry, :operation, :failed], _,
                    %{ferry: ^name, operation_id: ^id}}

    assert_receive {:telemetry, [:ferry, :operation, :dead_lettered], _,
                    %{ferry: ^name, operation_id: ^id}}
  end

  test "rejected push emits [:ferry, :operation, :rejected]" do
    name = start_ferry(max_queue_size: 1)
    {:ok, _} = Ferry.push(name, :a)
    {:error, :queue_full} = Ferry.push(name, :b)

    assert_receive {:telemetry, [:ferry, :operation, :rejected], %{queue_size: 1},
                    %{ferry: ^name, reason: :queue_full}}
  end

  test "pause/resume emit events" do
    name = start_ferry()
    :ok = Ferry.pause(name)
    assert_receive {:telemetry, [:ferry, :queue, :paused], _, %{ferry: ^name}}

    :ok = Ferry.resume(name)
    assert_receive {:telemetry, [:ferry, :queue, :resumed], _, %{ferry: ^name}}
  end

  test "DLQ retry emits event" do
    name = start_ferry(resolver: failure_resolver())
    {:ok, _} = Ferry.push(name, :payload)
    :ok = Ferry.flush(name)

    {:ok, 1} = Ferry.retry_dead_letters(name)
    assert_receive {:telemetry, [:ferry, :dlq, :retried], %{count: 1}, %{ferry: ^name}}
  end

  test "DLQ drain emits event" do
    name = start_ferry(resolver: failure_resolver())
    {:ok, _} = Ferry.push(name, :payload)
    :ok = Ferry.flush(name)

    :ok = Ferry.drain_dead_letters(name)
    assert_receive {:telemetry, [:ferry, :dlq, :drained], %{count: 1}, %{ferry: ^name}}
  end
end
