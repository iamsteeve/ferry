defmodule FerryDashboardWeb.FerryController do
  use FerryDashboardWeb, :controller

  alias FerryDashboard.Ferries.{InventoryFerry, EmailFerry, PositionsFerry, AiJobsFerry}

  @ferry_map %{
    "positions" => PositionsFerry,
    "emails" => EmailFerry,
    "inventory" => InventoryFerry,
    "ai-jobs" => AiJobsFerry
  }

  @ferry_configs %{
    PositionsFerry => %{
      key: "positions",
      label: "Position Updates",
      batch_size: 20,
      flush_interval: 1_000,
      max_queue_size: 2_000,
      operation_timeout: 10_000,
      max_completed: 500
    },
    EmailFerry => %{
      key: "emails",
      label: "Email Outbound",
      batch_size: 15,
      flush_interval: 2_000,
      max_queue_size: 1_000,
      operation_timeout: 20_000,
      max_completed: 300
    },
    InventoryFerry => %{
      key: "inventory",
      label: "Inventory Updates",
      batch_size: 8,
      flush_interval: 3_000,
      max_queue_size: 500,
      operation_timeout: 30_000,
      max_completed: 200
    },
    AiJobsFerry => %{
      key: "ai-jobs",
      label: "AI Jobs",
      batch_size: 5,
      flush_interval: 4_000,
      max_queue_size: 200,
      operation_timeout: 120_000,
      max_completed: 100
    }
  }

  def index(conn, _params) do
    ferries =
      Enum.map(@ferry_configs, fn {mod, config} ->
        stats = Ferry.stats(mod)

        %{
          key: config.key,
          label: config.label,
          config: %{
            batch_size: config.batch_size,
            flush_interval: config.flush_interval,
            max_queue_size: config.max_queue_size,
            operation_timeout: config.operation_timeout,
            max_completed: config.max_completed
          },
          stats: %{
            queue_size: stats.queue_size,
            dlq_size: stats.dlq_size,
            completed_size: stats.completed_size,
            total_pushed: stats.total_pushed,
            total_processed: stats.total_processed,
            total_failed: stats.total_failed,
            total_rejected: stats.total_rejected,
            batches_executed: stats.batches_executed,
            avg_batch_duration_ms: stats.avg_batch_duration_ms,
            status: stats.status
          }
        }
      end)

    json(conn, %{ferries: ferries})
  end

  def push(conn, %{"ferry_key" => key, "payloads" => payloads}) when is_list(payloads) do
    with {:ok, mod} <- lookup_ferry(key) do
      case Ferry.push_many(mod, payloads) do
        {:ok, ids} ->
          conn
          |> put_status(:created)
          |> json(%{ok: true, ids: ids, ferry: key, count: length(ids)})

        {:error, :queue_full} ->
          conn
          |> put_status(:too_many_requests)
          |> json(%{error: "queue_full", detail: "The #{key} ferry queue is full"})
      end
    else
      :error ->
        conn
        |> put_status(:not_found)
        |> json(%{error: "not_found", detail: "Unknown ferry: '#{key}'"})
    end
  end

  def push(conn, %{"ferry_key" => key, "payload" => payload}) when is_map(payload) do
    with {:ok, mod} <- lookup_ferry(key) do
      case Ferry.push(mod, payload) do
        {:ok, id} ->
          conn
          |> put_status(:created)
          |> json(%{ok: true, ids: [id], ferry: key, count: 1})

        {:error, :queue_full} ->
          conn
          |> put_status(:too_many_requests)
          |> json(%{error: "queue_full", detail: "The #{key} ferry queue is full"})
      end
    else
      :error ->
        conn
        |> put_status(:not_found)
        |> json(%{error: "not_found", detail: "Unknown ferry: '#{key}'"})
    end
  end

  def push(conn, %{"ferry_key" => key}) do
    case lookup_ferry(key) do
      {:ok, _mod} ->
        conn
        |> put_status(:unprocessable_entity)
        |> json(%{error: "invalid_request", detail: "Request must include 'payload' or 'payloads'"})

      :error ->
        conn
        |> put_status(:not_found)
        |> json(%{error: "not_found", detail: "Unknown ferry: '#{key}'"})
    end
  end

  defp lookup_ferry(key) do
    case Map.fetch(@ferry_map, key) do
      {:ok, mod} -> {:ok, mod}
      :error -> :error
    end
  end
end
