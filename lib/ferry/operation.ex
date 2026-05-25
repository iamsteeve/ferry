defmodule Ferry.Operation do
  @moduledoc """
  Represents a single operation in the Ferry queue.

  Each operation tracks its full lifecycle from push to completion or failure.
  """

  @type status :: :pending | :processing | :completed | :dead

  @type t :: %__MODULE__{
          id: String.t(),
          payload: term(),
          order: pos_integer(),
          status: status(),
          pushed_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          result: term() | nil,
          error: term() | nil,
          batch_id: String.t() | nil
        }

  @enforce_keys [:id, :order, :status, :pushed_at]
  defstruct [
    :id,
    :payload,
    :order,
    :status,
    :pushed_at,
    :completed_at,
    :result,
    :error,
    :batch_id
  ]

  @doc """
  Returns a lightweight projection of the operation with `payload`, `result`,
  and `error` cleared.

  Stores use this for terminal-state operations in the lookup `index` to avoid
  duplicating the heavy fields, which already live in the `completed`/`dlq`
  tables. `Ferry.Store.get/2` hydrates back to the full operation on read.
  """
  @spec lite(t()) :: t()
  def lite(%__MODULE__{} = op) do
    %__MODULE__{op | payload: nil, result: nil, error: nil}
  end
end
