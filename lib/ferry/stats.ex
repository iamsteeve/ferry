defmodule Ferry.Stats do
  @moduledoc """
  Snapshot of a Ferry instance's runtime statistics.
  """

  @type t :: %__MODULE__{
          queue_size: non_neg_integer(),
          dlq_size: non_neg_integer(),
          completed_size: non_neg_integer(),
          total_pushed: non_neg_integer(),
          total_processed: non_neg_integer(),
          total_failed: non_neg_integer(),
          total_rejected: non_neg_integer(),
          batches_executed: non_neg_integer(),
          avg_batch_duration_ms: float(),
          last_flush_at: DateTime.t() | nil,
          uptime_ms: non_neg_integer(),
          memory_bytes: non_neg_integer(),
          status: :running | :paused
        }

  defstruct queue_size: 0,
            dlq_size: 0,
            completed_size: 0,
            total_pushed: 0,
            total_processed: 0,
            total_failed: 0,
            total_rejected: 0,
            batches_executed: 0,
            avg_batch_duration_ms: 0.0,
            last_flush_at: nil,
            uptime_ms: 0,
            memory_bytes: 0,
            status: :running
end
