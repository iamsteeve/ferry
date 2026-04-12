defmodule FerryDashboard.Ferries.PositionsResolver do
  @moduledoc """
  Simulates GPS/position update processing.
  Very fast processing (50-150ms), low failure rate (~3%).
  """

  def resolve(%Ferry.Batch{operations: ops}) do
    Enum.map(ops, fn op ->
      Process.sleep(Enum.random(50..150))

      if :rand.uniform(100) <= 3 do
        {op.id, :error, :gps_signal_lost}
      else
        lat = op.payload[:lat] || Float.round(:rand.uniform() * 180 - 90, 4)
        lng = op.payload[:lng] || Float.round(:rand.uniform() * 360 - 180, 4)
        {op.id, :ok, %{lat: lat, lng: lng, accuracy_m: :rand.uniform(50), synced: true}}
      end
    end)
  end
end
