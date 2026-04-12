defmodule FerryDashboard.Ferries.AiJobsFerry do
  use Ferry,
    resolver: &FerryDashboard.Ferries.AiJobsResolver.resolve/1,
    batch_size: 5,
    flush_interval: 4_000,
    max_queue_size: 200,
    operation_timeout: :timer.minutes(2),
    max_completed: 100,
    auto_flush: true
end
