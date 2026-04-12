defmodule FerryDashboard.Ferries.EmailFerry do
  use Ferry,
    resolver: &FerryDashboard.Ferries.EmailResolver.resolve/1,
    batch_size: 15,
    flush_interval: 2_000,
    max_queue_size: 1_000,
    operation_timeout: :timer.seconds(20),
    max_completed: 300,
    auto_flush: true
end
