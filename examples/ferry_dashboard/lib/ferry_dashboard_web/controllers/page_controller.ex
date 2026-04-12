defmodule FerryDashboardWeb.PageController do
  use FerryDashboardWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
