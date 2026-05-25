defmodule Ferry.Store do
  @moduledoc """
  Behaviour for Ferry storage backends.

  Implementations manage the pending queue, completed history, and dead-letter queue.
  """

  @type state :: term()

  @callback init(ferry_name :: atom(), opts :: keyword()) :: {:ok, state}

  @callback push(state, Ferry.Operation.t()) :: {:ok, state}

  @callback push_many(state, [Ferry.Operation.t()]) :: {:ok, state}

  @callback pop_batch(state, non_neg_integer()) :: {[Ferry.Operation.t()], state}

  @callback get(state, String.t()) :: {:ok, Ferry.Operation.t()} | {:error, :not_found}

  @callback mark_completed(
              state,
              String.t(),
              result :: term(),
              DateTime.t(),
              batch_id :: String.t() | nil
            ) ::
              {:ok, state}

  @callback mark_failed(
              state,
              String.t(),
              error :: term(),
              DateTime.t(),
              batch_id :: String.t() | nil
            ) ::
              {:ok, state}

  @callback move_to_dlq(state, String.t(), error :: term()) :: {:ok, state}

  @callback move_batch_to_dlq(state, [Ferry.Operation.t()], error :: term()) :: {:ok, state}

  @callback list_pending(state, keyword()) :: [Ferry.Operation.t()]

  @callback list_completed(state, keyword()) :: [Ferry.Operation.t()]

  @callback list_dlq(state) :: [Ferry.Operation.t()]

  @callback dlq_size(state) :: non_neg_integer()

  @callback retry_all_dlq(state) :: {non_neg_integer(), state}

  @callback drain_dlq(state) :: {non_neg_integer(), state}

  @callback queue_size(state) :: non_neg_integer()

  @callback completed_size(state) :: non_neg_integer()

  @callback purge_completed(state, max_age_ms :: integer(), max_count :: integer()) ::
              {non_neg_integer(), state}

  @callback drain_completed(state) :: {non_neg_integer(), state}

  @callback clear_queue(state, error :: term()) :: {non_neg_integer(), state}

  @callback delete(state, String.t()) :: {:ok | {:error, :not_found}, state}

  @callback memory_bytes(state) :: non_neg_integer()
end
