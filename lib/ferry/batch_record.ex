defmodule Ferry.BatchRecord do
  @moduledoc """
  Lightweight record of an executed batch, used for batch history tracking.

  Unlike `Ferry.Batch` (which carries full operation structs and is transient),
  a `BatchRecord` stores only operation IDs and aggregate metadata for historical queries.
  """

  @type status :: :started | :completed | :timeout | :crashed

  @type t :: %__MODULE__{
          id: String.t(),
          operation_ids: [String.t()],
          size: non_neg_integer(),
          trigger: :timer | :manual | atom(),
          status: status(),
          started_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          succeeded: non_neg_integer(),
          failed: non_neg_integer()
        }

  @enforce_keys [:id, :operation_ids, :size, :trigger, :status, :started_at]
  defstruct [
    :id,
    :operation_ids,
    :size,
    :trigger,
    :status,
    :started_at,
    :completed_at,
    :duration_ms,
    succeeded: 0,
    failed: 0
  ]
end
