defmodule FerryDashboard.Ferries.PositionsFerry do
  use Ferry,
    resolver: &FerryDashboard.Ferries.PositionsResolver.resolve/1,
    batch_size: 20,
    flush_interval: 1_000,
    max_queue_size: 2_000,
    operation_timeout: :timer.seconds(10),
    max_completed: 500,
    auto_flush: true
end
