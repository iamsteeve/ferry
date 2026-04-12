defmodule FerryDashboard.Ferries.InventoryFerry do
  use Ferry,
    resolver: &FerryDashboard.Ferries.InventoryResolver.resolve/1,
    batch_size: 50,
    flush_interval: 500,
    max_queue_size: 10_000,
    operation_timeout: :timer.seconds(15),
    max_completed: 50,
    auto_flush: true
end
