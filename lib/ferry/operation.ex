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

  @enforce_keys [:id, :payload, :order, :status, :pushed_at]
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
end
