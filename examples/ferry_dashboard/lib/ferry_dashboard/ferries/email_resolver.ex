defmodule FerryDashboard.Ferries.EmailResolver do
  @moduledoc """
  Simulates email delivery processing.
  Batch-style: single delay for the whole batch, ~5% individual failure.
  """

  def resolve(%Ferry.Batch{operations: ops}) do
    # Simulate SMTP connection overhead
    Process.sleep(Enum.random(200..600))

    Enum.map(ops, fn op ->
      if :rand.uniform(100) <= 5 do
        {op.id, :error, :mailbox_full}
      else
        to = op.payload[:to] || "user#{:rand.uniform(999)}@example.com"
        {op.id, :ok, %{delivered_to: to, message_id: "msg_#{Ferry.IdGenerator.generate(nil)}"}}
      end
    end)
  end
end
