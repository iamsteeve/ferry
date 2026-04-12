defmodule FerryDashboardWeb.Router do
  use FerryDashboardWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {FerryDashboardWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", FerryDashboardWeb do
    pipe_through :browser

    live_session :dashboard, layout: false do
      live "/", DashboardLive
    end
  end

  scope "/api", FerryDashboardWeb do
    pipe_through :api

    get "/ferries", FerryController, :index
    post "/:ferry_key", FerryController, :push
  end
end
