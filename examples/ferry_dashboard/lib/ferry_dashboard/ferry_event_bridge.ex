defmodule FerryDashboard.FerryEventBridge do
  @moduledoc """
  Attaches telemetry handlers for all Ferry events and broadcasts them
  via Phoenix.PubSub so LiveViews can receive real-time updates.
  """

  @pubsub FerryDashboard.PubSub
  @topic "ferry:events"

  def topic, do: @topic

  def attach do
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
      [:ferry, :dlq, :drained],
      [:ferry, :completed, :purged]
    ]

    :telemetry.attach_many(
      "ferry-dashboard-bridge",
      events,
      &__MODULE__.handle_event/4,
      nil
    )
  end

  def handle_event(event, measurements, metadata, _config) do
    Phoenix.PubSub.broadcast(
      @pubsub,
      @topic,
      {:ferry_event, event, measurements, metadata}
    )
  end
end
