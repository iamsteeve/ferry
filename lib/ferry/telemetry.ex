defmodule Ferry.Telemetry do
  @moduledoc """
  Telemetry event emission helpers for Ferry.

  All events are prefixed with `[:ferry]`. The ferry instance name
  is always included in metadata.
  """

  @doc false
  def emit_pushed(ferry_name, operation_id, queue_size) do
    :telemetry.execute(
      [:ferry, :operation, :pushed],
      %{queue_size: queue_size},
      %{ferry: ferry_name, operation_id: operation_id}
    )
  end

  @doc false
  def emit_completed(ferry_name, operation_id, batch_id, duration_ms) do
    :telemetry.execute(
      [:ferry, :operation, :completed],
      %{duration_ms: duration_ms},
      %{ferry: ferry_name, operation_id: operation_id, batch_id: batch_id}
    )
  end

  @doc false
  def emit_failed(ferry_name, operation_id, batch_id, error, duration_ms) do
    :telemetry.execute(
      [:ferry, :operation, :failed],
      %{duration_ms: duration_ms},
      %{ferry: ferry_name, operation_id: operation_id, error: error, batch_id: batch_id}
    )
  end

  @doc false
  def emit_dead_lettered(ferry_name, operation_id, error) do
    :telemetry.execute(
      [:ferry, :operation, :dead_lettered],
      %{},
      %{ferry: ferry_name, operation_id: operation_id, error: error}
    )
  end

  @doc false
  def emit_rejected(ferry_name, queue_size) do
    :telemetry.execute(
      [:ferry, :operation, :rejected],
      %{queue_size: queue_size},
      %{ferry: ferry_name, reason: :queue_full}
    )
  end

  @doc false
  def emit_batch_started(
        ferry_name,
        batch_id,
        batch_size,
        queue_size_before,
        trigger,
        operation_ids
      ) do
    :telemetry.execute(
      [:ferry, :batch, :started],
      %{batch_size: batch_size, queue_size_before: queue_size_before},
      %{ferry: ferry_name, batch_id: batch_id, trigger: trigger, operation_ids: operation_ids}
    )
  end

  @doc false
  def emit_batch_completed(
        ferry_name,
        batch_id,
        batch_size,
        succeeded,
        failed,
        duration_ms,
        opts \\ []
      ) do
    metadata =
      %{ferry: ferry_name, batch_id: batch_id}
      |> maybe_put(:status_override, Keyword.get(opts, :status_override))

    :telemetry.execute(
      [:ferry, :batch, :completed],
      %{batch_size: batch_size, succeeded: succeeded, failed: failed, duration_ms: duration_ms},
      metadata
    )
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @doc false
  def emit_batch_timeout(ferry_name, batch_id, batch_size, timeout_ms) do
    :telemetry.execute(
      [:ferry, :batch, :timeout],
      %{batch_size: batch_size, timeout_ms: timeout_ms},
      %{ferry: ferry_name, batch_id: batch_id}
    )
  end

  @doc false
  def emit_paused(ferry_name) do
    :telemetry.execute([:ferry, :queue, :paused], %{}, %{ferry: ferry_name})
  end

  @doc false
  def emit_resumed(ferry_name) do
    :telemetry.execute([:ferry, :queue, :resumed], %{}, %{ferry: ferry_name})
  end

  @doc false
  def emit_dlq_retried(ferry_name, count) do
    :telemetry.execute(
      [:ferry, :dlq, :retried],
      %{count: count},
      %{ferry: ferry_name}
    )
  end

  @doc false
  def emit_dlq_drained(ferry_name, count) do
    :telemetry.execute(
      [:ferry, :dlq, :drained],
      %{count: count},
      %{ferry: ferry_name}
    )
  end

  @doc false
  def emit_completed_purged(ferry_name, count, reason) do
    :telemetry.execute(
      [:ferry, :completed, :purged],
      %{count: count},
      %{ferry: ferry_name, reason: reason}
    )
  end

  @doc false
  def emit_batch_crashed(ferry_name, batch_id, batch_size, reason) do
    :telemetry.execute(
      [:ferry, :batch, :crashed],
      %{batch_size: batch_size},
      %{ferry: ferry_name, batch_id: batch_id, reason: reason}
    )
  end

  @doc false
  def emit_queue_cleared(ferry_name, count) do
    :telemetry.execute(
      [:ferry, :queue, :cleared],
      %{count: count},
      %{ferry: ferry_name}
    )
  end

  @doc false
  def emit_completed_drained(ferry_name, count) do
    :telemetry.execute(
      [:ferry, :completed, :drained],
      %{count: count},
      %{ferry: ferry_name}
    )
  end

  @doc false
  def emit_operation_deleted(ferry_name, operation_id) do
    :telemetry.execute(
      [:ferry, :operation, :deleted],
      %{},
      %{ferry: ferry_name, operation_id: operation_id}
    )
  end
end
