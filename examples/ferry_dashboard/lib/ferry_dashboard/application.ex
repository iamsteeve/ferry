defmodule FerryDashboard.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Attach telemetry bridge before starting Ferries
    FerryDashboard.FerryEventBridge.attach()

    children = [
      FerryDashboardWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:ferry_dashboard, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: FerryDashboard.PubSub},

      # Ferry instances
      FerryDashboard.Ferries.InventoryFerry,
      FerryDashboard.Ferries.EmailFerry,
      FerryDashboard.Ferries.PositionsFerry,
      FerryDashboard.Ferries.AiJobsFerry,

      # Web endpoint
      FerryDashboardWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: FerryDashboard.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    FerryDashboardWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
