defmodule FerryDashboard.Ferries.InventoryResolver do
  @moduledoc """
  Simulates high-throughput inventory adjustment processing.
  Processes batches quickly (10-30ms per op), ~4% failure rate.
  """

  def resolve(%Ferry.Batch{operations: ops}) do
    Enum.map(ops, fn op ->
      Process.sleep(Enum.random(10..30))

      if :rand.uniform(100) <= 4 do
        {op.id, :error, :warehouse_unavailable}
      else
        sku = op.payload[:sku] || "SKU-#{:rand.uniform(9999)}"
        warehouse = "WH-#{Enum.random(~w(US-EAST US-WEST EU-CENTRAL AP-SOUTH))}"
        {op.id, :ok, %{sku: sku, adjusted: true, warehouse: warehouse, qty_delta: Enum.random(-50..50)}}
      end
    end)
  end
end
