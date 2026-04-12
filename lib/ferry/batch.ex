defmodule Ferry.Batch do
  @moduledoc """
  Represents a batch of operations sent to the resolver for processing.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          operations: [Ferry.Operation.t()],
          size: non_neg_integer(),
          ferry_name: atom(),
          flushed_at: DateTime.t(),
          metadata: map()
        }

  @enforce_keys [:id, :operations, :size, :ferry_name, :flushed_at]
  defstruct [:id, :operations, :size, :ferry_name, :flushed_at, metadata: %{}]
end
