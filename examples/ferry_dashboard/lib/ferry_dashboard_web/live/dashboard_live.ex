defmodule FerryDashboardWeb.DashboardLive do
  use FerryDashboardWeb, :live_view

  alias FerryDashboard.Ferries.{InventoryFerry, EmailFerry, PositionsFerry, AiJobsFerry}

  @ferries [
    %{
      module: InventoryFerry,
      label: "Inventory Notifications",
      icon: :inventory,
      color: "amber",
      description: "Warehouse stock adjustments & alerts"
    },
    %{
      module: EmailFerry,
      label: "Email Outbound",
      icon: :email,
      color: "blue",
      description: "Transactional & notification emails"
    },
    %{
      module: PositionsFerry,
      label: "Position Updates",
      icon: :positions,
      color: "emerald",
      description: "GPS & location tracking sync"
    },
    %{
      module: AiJobsFerry,
      label: "AI Job Queue",
      icon: :ai_jobs,
      color: "violet",
      description: "Bulk LLM inference & embeddings"
    }
  ]

  @refresh_interval 800
  @sim_tick_interval 200

  @ferry_api_keys %{
    PositionsFerry => "positions",
    EmailFerry => "emails",
    InventoryFerry => "inventory",
    AiJobsFerry => "ai-jobs"
  }

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(FerryDashboard.PubSub, FerryDashboard.FerryEventBridge.topic())
      Process.send_after(self(), :refresh_stats, @refresh_interval)
    end

    sim_config =
      Map.new(@ferries, fn %{module: mod} ->
        {mod,
         %{
           enabled: false,
           interval_ms: default_sim_interval(mod),
           batch_size: default_sim_batch(mod),
           next_at: nil
         }}
      end)

    ferries =
      Enum.map(@ferries, fn ferry ->
        Map.put(ferry, :config, Ferry.config(ferry.module))
      end)

    socket =
      socket
      |> assign(:ferries, ferries)
      |> assign(:ferry_stats, load_all_stats())
      |> assign(:recent_events, [])
      |> assign(:selected_ferry, nil)
      |> assign(:detail_tab, :operations)
      |> assign(:sim_config, sim_config)
      |> assign(:sim_running, false)
      |> assign(:sim_panel_open, true)
      |> assign(:pending_ops, [])
      |> assign(:completed_ops, [])
      |> assign(:dead_letters, [])
      |> assign(:new_pending_ids, MapSet.new())
      |> assign(:new_completed_ids, MapSet.new())
      |> assign(:new_dead_ids, MapSet.new())

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh_stats, socket) do
    Process.send_after(self(), :refresh_stats, @refresh_interval)

    socket =
      socket
      |> assign(:ferry_stats, load_all_stats())
      |> refresh_detail_data()

    {:noreply, socket}
  end

  @impl true
  def handle_info(:sim_tick, socket) do
    if socket.assigns.sim_running do
      Process.send_after(self(), :sim_tick, @sim_tick_interval)
      now = System.monotonic_time(:millisecond)

      sim_config =
        Enum.reduce(socket.assigns.sim_config, socket.assigns.sim_config, fn {mod, cfg}, acc ->
          if cfg.enabled and (cfg.next_at == nil or now >= cfg.next_at) do
            payloads = for i <- 1..cfg.batch_size, do: generate_payload(mod, i)
            post_to_api(mod, payloads)
            Map.put(acc, mod, %{cfg | next_at: now + cfg.interval_ms})
          else
            acc
          end
        end)

      {:noreply, assign(socket, :sim_config, sim_config)}
    else
      {:noreply, socket}
    end
  end

  @impl true
  def handle_info({:ferry_event, event, measurements, metadata}, socket) do
    entry = %{
      id: System.unique_integer([:positive]),
      event: event,
      measurements: measurements,
      metadata: metadata,
      timestamp: DateTime.utc_now()
    }

    recent = Enum.take([entry | socket.assigns.recent_events], 200)

    socket =
      socket
      |> assign(:recent_events, recent)
      |> maybe_refresh_detail(metadata)

    {:noreply, socket}
  end

  @impl true
  def handle_event("push", %{"ferry" => ferry_str, "count" => count_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    count = String.to_integer(count_str)

    payloads = for i <- 1..count, do: generate_payload(ferry, i)
    post_to_api(ferry, payloads)

    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("flush", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    Ferry.flush(ferry)
    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("pause", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    Ferry.pause(ferry)
    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("resume", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    Ferry.resume(ferry)
    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("retry_dlq", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    Ferry.retry_dead_letters(ferry)
    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("drain_dlq", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)
    Ferry.drain_dead_letters(ferry)
    {:noreply, socket |> assign(:ferry_stats, load_all_stats()) |> refresh_detail_data()}
  end

  @impl true
  def handle_event("select_ferry", %{"ferry" => ferry_str}, socket) do
    ferry = String.to_existing_atom(ferry_str)

    selected =
      if socket.assigns.selected_ferry == ferry,
        do: nil,
        else: ferry

    socket =
      socket
      |> assign(:selected_ferry, selected)
      |> reset_detail_data()
      |> refresh_detail_data()

    {:noreply, socket}
  end

  @impl true
  def handle_event("detail_tab", %{"tab" => tab}, socket) do
    {:noreply, assign(socket, :detail_tab, String.to_existing_atom(tab))}
  end

  # ── Simulation events ──

  @impl true
  def handle_event("sim_toggle_master", _params, socket) do
    running = !socket.assigns.sim_running

    socket =
      if running do
        Process.send_after(self(), :sim_tick, @sim_tick_interval)
        # Reset all next_at so they fire immediately
        sim_config =
          Map.new(socket.assigns.sim_config, fn {mod, cfg} ->
            {mod, %{cfg | next_at: nil}}
          end)

        socket |> assign(:sim_running, true) |> assign(:sim_config, sim_config)
      else
        assign(socket, :sim_running, false)
      end

    {:noreply, socket}
  end

  @impl true
  def handle_event("sim_toggle_queue", %{"ferry" => ferry_str}, socket) do
    mod = String.to_existing_atom(ferry_str)
    sim_config = socket.assigns.sim_config
    cfg = sim_config[mod]
    updated = %{cfg | enabled: !cfg.enabled, next_at: nil}
    {:noreply, assign(socket, :sim_config, Map.put(sim_config, mod, updated))}
  end

  @impl true
  def handle_event("sim_update_config", %{"ferry" => ferry_str} = params, socket) do
    mod = String.to_existing_atom(ferry_str)
    sim_config = socket.assigns.sim_config
    cfg = sim_config[mod]

    cfg =
      cfg
      |> then(fn c ->
        case params do
          %{"interval" => val} ->
            {ms, _} = Integer.parse(val)
            %{c | interval_ms: max(ms, 200)}

          _ ->
            c
        end
      end)
      |> then(fn c ->
        case params do
          %{"batch" => val} ->
            {n, _} = Integer.parse(val)
            %{c | batch_size: min(max(n, 1), 100)}

          _ ->
            c
        end
      end)

    {:noreply, assign(socket, :sim_config, Map.put(sim_config, mod, cfg))}
  end

  @impl true
  def handle_event("sim_toggle_panel", _params, socket) do
    {:noreply, assign(socket, :sim_panel_open, !socket.assigns.sim_panel_open)}
  end

  @impl true
  def handle_event("clear_all", _params, socket) do
    for %{module: mod} <- @ferries do
      Ferry.drain_dead_letters(mod)
    end

    {:noreply,
     socket
     |> assign(:recent_events, [])
     |> assign(:ferry_stats, load_all_stats())
     |> refresh_detail_data()}
  end

  # Private

  defp default_sim_interval(InventoryFerry), do: 2000
  defp default_sim_interval(EmailFerry), do: 1000
  defp default_sim_interval(PositionsFerry), do: 500
  defp default_sim_interval(AiJobsFerry), do: 3000

  defp default_sim_batch(InventoryFerry), do: 3
  defp default_sim_batch(EmailFerry), do: 5
  defp default_sim_batch(PositionsFerry), do: 8
  defp default_sim_batch(AiJobsFerry), do: 2

  defp load_all_stats do
    Map.new(@ferries, fn %{module: mod} ->
      {mod, Ferry.stats(mod)}
    end)
  end

  defp refresh_detail_data(socket) do
    case socket.assigns.selected_ferry do
      nil ->
        socket

      ferry ->
        pending = Ferry.pending(ferry, limit: 30)
        completed = Ferry.completed(ferry, limit: 30)
        dead = Ferry.dead_letters(ferry)

        old_pending_ids = MapSet.new(socket.assigns.pending_ops, & &1.id)
        old_completed_ids = MapSet.new(socket.assigns.completed_ops, & &1.id)
        old_dead_ids = MapSet.new(socket.assigns.dead_letters, & &1.id)

        new_pending_ids =
          pending |> MapSet.new(& &1.id) |> MapSet.difference(old_pending_ids)

        new_completed_ids =
          completed |> MapSet.new(& &1.id) |> MapSet.difference(old_completed_ids)

        new_dead_ids =
          dead |> MapSet.new(& &1.id) |> MapSet.difference(old_dead_ids)

        socket
        |> assign(:pending_ops, pending)
        |> assign(:completed_ops, completed)
        |> assign(:dead_letters, dead)
        |> assign(:new_pending_ids, new_pending_ids)
        |> assign(:new_completed_ids, new_completed_ids)
        |> assign(:new_dead_ids, new_dead_ids)
    end
  end

  defp reset_detail_data(socket) do
    socket
    |> assign(:pending_ops, [])
    |> assign(:completed_ops, [])
    |> assign(:dead_letters, [])
    |> assign(:new_pending_ids, MapSet.new())
    |> assign(:new_completed_ids, MapSet.new())
    |> assign(:new_dead_ids, MapSet.new())
  end

  defp maybe_refresh_detail(socket, %{ferry: ferry}) do
    if socket.assigns.selected_ferry == ferry do
      refresh_detail_data(socket)
    else
      socket
    end
  end

  defp maybe_refresh_detail(socket, _metadata), do: socket

  defp generate_payload(mod, _i) do
    case mod do
      InventoryFerry ->
        %{
          sku: "SKU-#{:rand.uniform(9999)}",
          action: Enum.random([:restock, :adjust, :transfer, :count]),
          quantity: :rand.uniform(500),
          warehouse: "WH-#{:rand.uniform(5)}"
        }

      EmailFerry ->
        %{
          to: "user#{:rand.uniform(9999)}@example.com",
          subject: Enum.random(["Order confirmed", "Shipped!", "Welcome", "Reset password"]),
          template: Enum.random([:transactional, :marketing, :alert])
        }

      PositionsFerry ->
        %{
          vehicle_id: "VH-#{:rand.uniform(200)}",
          lat: Float.round(:rand.uniform() * 180 - 90, 4),
          lng: Float.round(:rand.uniform() * 360 - 180, 4),
          speed_kmh: :rand.uniform(120)
        }

      AiJobsFerry ->
        %{
          model: Enum.random(["claude-4", "gpt-5", "gemini-3", "llama-4"]),
          task: Enum.random([:summarize, :classify, :embed, :generate, :extract]),
          tokens: :rand.uniform(4000) + 500,
          priority: Enum.random([:low, :normal, :high])
        }
    end
  end

  defp post_to_api(mod, payloads) do
    key = @ferry_api_keys[mod]
    port = FerryDashboardWeb.Endpoint.config(:http) |> Keyword.get(:port, 4000)
    url = ~c"http://127.0.0.1:#{port}/api/#{key}"
    body = Jason.encode!(%{"payloads" => payloads})
    headers = [{~c"content-type", ~c"application/json"}]
    :httpc.request(:post, {url, headers, ~c"application/json", body}, [], [])
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="min-h-screen bg-zinc-950 text-zinc-100">
      <!-- Header -->
      <header class="border-b border-zinc-800/50 bg-zinc-950/80 backdrop-blur-xl sticky top-0 z-50">
        <div class="max-w-[1600px] mx-auto px-6 py-4 flex items-center justify-between">
          <div class="flex items-center gap-3">
            <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-sm font-bold">
              F
            </div>
            <div>
              <h1 class="text-lg font-semibold tracking-tight">Ferry Dashboard</h1>
              <p class="text-xs text-zinc-500">Real-time operation queue monitor</p>
            </div>
          </div>
          <div class="flex items-center gap-4">
            <div class="flex items-center gap-2 text-xs text-zinc-500">
              <span class="relative flex h-2 w-2">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75">
                </span>
                <span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              Live
            </div>
            <.global_stats ferry_stats={@ferry_stats} />
            <button
              phx-click="clear_all"
              class="px-3 py-1.5 rounded-lg bg-zinc-800/50 hover:bg-red-500/10 border border-zinc-700/30 hover:border-red-500/20 text-[11px] text-zinc-500 hover:text-red-400 transition-all duration-200"
            >
              <svg
                class="w-3.5 h-3.5 inline-block mr-1 -mt-0.5"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
              </svg>
              Clear
            </button>
          </div>
        </div>
      </header>

      <main class="max-w-[1600px] mx-auto px-6 py-6">
        <!-- Simulation Panel -->
        <.sim_panel
          sim_config={@sim_config}
          sim_running={@sim_running}
          sim_panel_open={@sim_panel_open}
          ferries={@ferries}
        />
        
    <!-- Queue Cards Grid -->
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
          <.ferry_card
            :for={ferry <- @ferries}
            ferry={ferry}
            stats={@ferry_stats[ferry.module]}
            selected={@selected_ferry == ferry.module}
          />
        </div>
        
    <!-- Detail Panel + Event Feed -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <!-- Selected Ferry Detail -->
          <div class="lg:col-span-2">
            <%= if @selected_ferry do %>
              <.ferry_detail
                ferry={Enum.find(@ferries, &(&1.module == @selected_ferry))}
                stats={@ferry_stats[@selected_ferry]}
                detail_tab={@detail_tab}
                dead_letters={@dead_letters}
                pending_ops={@pending_ops}
                completed_ops={@completed_ops}
                ferry_events={filter_events_for(@recent_events, @selected_ferry)}
                new_pending_ids={@new_pending_ids}
                new_completed_ids={@new_completed_ids}
                new_dead_ids={@new_dead_ids}
              />
            <% else %>
              <div class="rounded-xl border border-zinc-800/50 bg-zinc-900/30 p-12 text-center">
                <p class="text-zinc-600 text-sm">Select a queue to view details</p>
              </div>
            <% end %>
          </div>
          
    <!-- Live Event Feed -->
          <div class="lg:col-span-1">
            <.event_feed events={@recent_events} />
          </div>
        </div>
      </main>
    </div>
    """
  end

  # ── Components ──

  defp global_stats(assigns) do
    totals =
      Enum.reduce(assigns.ferry_stats, %{pushed: 0, processed: 0, failed: 0, queued: 0}, fn {_k,
                                                                                             s},
                                                                                            acc ->
        %{
          acc
          | pushed: acc.pushed + s.total_pushed,
            processed: acc.processed + s.total_processed,
            failed: acc.failed + s.total_failed,
            queued: acc.queued + s.queue_size
        }
      end)

    assigns = assign(assigns, :totals, totals)

    ~H"""
    <div class="flex items-center gap-4 text-xs">
      <div class="px-3 py-1.5 rounded-lg bg-zinc-800/50 border border-zinc-700/30">
        <span class="text-zinc-400">Pushed</span>
        <span class="ml-1.5 font-mono font-medium text-zinc-200">
          {format_number(@totals.pushed)}
        </span>
      </div>
      <div class="px-3 py-1.5 rounded-lg bg-zinc-800/50 border border-zinc-700/30">
        <span class="text-zinc-400">Processed</span>
        <span class="ml-1.5 font-mono font-medium text-emerald-400">
          {format_number(@totals.processed)}
        </span>
      </div>
      <div class="px-3 py-1.5 rounded-lg bg-zinc-800/50 border border-zinc-700/30">
        <span class="text-zinc-400">Failed</span>
        <span class="ml-1.5 font-mono font-medium text-red-400">{format_number(@totals.failed)}</span>
      </div>
      <div class="px-3 py-1.5 rounded-lg bg-zinc-800/50 border border-zinc-700/30">
        <span class="text-zinc-400">Queued</span>
        <span class="ml-1.5 font-mono font-medium text-amber-400">
          {format_number(@totals.queued)}
        </span>
      </div>
    </div>
    """
  end

  defp sim_panel(assigns) do
    any_enabled = Enum.any?(assigns.sim_config, fn {_mod, cfg} -> cfg.enabled end)
    assigns = assign(assigns, :any_enabled, any_enabled)

    ~H"""
    <div class="mb-6 rounded-xl border border-zinc-800/50 bg-zinc-900/30 overflow-hidden">
      <!-- Panel Header (clickable to toggle) -->
      <button
        phx-click="sim_toggle_panel"
        class="w-full px-5 py-3 flex items-center justify-between hover:bg-zinc-800/20 transition-colors duration-200"
      >
        <div class="flex items-center gap-3">
          <div class={"w-7 h-7 rounded-lg flex items-center justify-center " <>
            if(@sim_running and @any_enabled,
              do: "bg-cyan-500/15 text-cyan-400",
              else: "bg-zinc-800/50 text-zinc-500"
            )}>
            <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
            </svg>
          </div>
          <div class="text-left">
            <h3 class="text-sm font-medium text-zinc-300">Data Simulator</h3>
            <p class="text-[10px] text-zinc-600">
              Auto-push mock payloads to test queues in real-time
            </p>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <%= if @sim_running and @any_enabled do %>
            <div class="flex items-center gap-1.5 text-[10px] text-cyan-400 font-medium">
              <span class="relative flex h-2 w-2">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75">
                </span>
                <span class="relative inline-flex rounded-full h-2 w-2 bg-cyan-500"></span>
              </span>
              Simulating
            </div>
          <% end %>
          <span class={"text-zinc-600 text-xs transition-transform duration-200 " <>
            if(@sim_panel_open, do: "rotate-180", else: "")}>
            ▾
          </span>
        </div>
      </button>
      
    <!-- Collapsible Content -->
      <%= if @sim_panel_open do %>
        <div class="border-t border-zinc-800/50 animate-slide-up">
          <!-- Master toggle -->
          <div class="px-5 py-3 border-b border-zinc-800/30 flex items-center justify-between bg-zinc-900/50">
            <span class="text-xs text-zinc-400">Master switch</span>
            <button
              phx-click="sim_toggle_master"
              class={"relative inline-flex h-6 w-11 items-center rounded-full transition-colors duration-300 focus:outline-none " <>
                if(@sim_running,
                  do: "bg-cyan-500",
                  else: "bg-zinc-700"
                )}
            >
              <span class={"inline-block h-4 w-4 transform rounded-full bg-white shadow-lg transition-transform duration-300 " <>
                if(@sim_running, do: "translate-x-6", else: "translate-x-1")} />
            </button>
          </div>
          
    <!-- Per-queue config grid -->
          <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 divide-x divide-zinc-800/30">
            <.sim_queue_config
              :for={ferry <- @ferries}
              ferry={ferry}
              config={@sim_config[ferry.module]}
              sim_running={@sim_running}
            />
          </div>
        </div>
      <% end %>
    </div>
    """
  end

  defp sim_queue_config(assigns) do
    active = assigns.config.enabled and assigns.sim_running

    assigns =
      assigns
      |> assign(:active, active)
      |> assign(:color_classes, color_classes(assigns.ferry.color))

    ~H"""
    <div class={"p-4 transition-colors duration-300 " <> if(@active, do: "bg-zinc-800/20", else: "")}>
      <!-- Queue header with toggle -->
      <div class="flex items-center justify-between mb-3">
        <div class="flex items-center gap-2">
          <.ferry_icon name={@ferry.icon} class="w-4 h-4" />
          <span class="text-xs font-medium text-zinc-300">{@ferry.label}</span>
        </div>
        <button
          phx-click="sim_toggle_queue"
          phx-value-ferry={@ferry.module}
          class={"relative inline-flex h-5 w-9 items-center rounded-full transition-colors duration-300 focus:outline-none " <>
            if(@config.enabled,
              do: "bg-cyan-500/80",
              else: "bg-zinc-700"
            )}
        >
          <span class={"inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform duration-300 " <>
            if(@config.enabled, do: "translate-x-[18px]", else: "translate-x-[3px]")} />
        </button>
      </div>
      
    <!-- Config form -->
      <form phx-change="sim_update_config" phx-value-ferry={@ferry.module}>
        <div class="space-y-2.5">
          <div>
            <label class="flex items-center justify-between text-[10px] text-zinc-600 mb-1">
              <span class="uppercase tracking-wider">Interval</span>
              <span class="font-mono text-zinc-500">{@config.interval_ms}ms</span>
            </label>
            <input
              type="range"
              min="200"
              max="10000"
              step="100"
              value={@config.interval_ms}
              name="interval"
              class="w-full h-1 bg-zinc-700 rounded-full appearance-none cursor-pointer accent-cyan-500"
            />
            <div class="flex justify-between text-[9px] text-zinc-700 mt-0.5">
              <span>200ms</span>
              <span>10s</span>
            </div>
          </div>

          <div>
            <label class="flex items-center justify-between text-[10px] text-zinc-600 mb-1">
              <span class="uppercase tracking-wider">Batch size</span>
              <span class="font-mono text-zinc-500">{@config.batch_size} ops</span>
            </label>
            <input
              type="range"
              min="1"
              max="50"
              step="1"
              value={@config.batch_size}
              name="batch"
              class="w-full h-1 bg-zinc-700 rounded-full appearance-none cursor-pointer accent-cyan-500"
            />
            <div class="flex justify-between text-[9px] text-zinc-700 mt-0.5">
              <span>1</span>
              <span>50</span>
            </div>
          </div>
        </div>
      </form>
      
    <!-- Throughput indicator -->
      <%= if @active do %>
        <div class="pt-2 flex items-center gap-1.5 text-[10px] text-cyan-400/70">
          <span class="w-1 h-1 rounded-full bg-cyan-400 animate-pulse" />
          <span class="font-mono">
            ~{Float.round(@config.batch_size / (@config.interval_ms / 1000), 1)} ops/s
          </span>
        </div>
      <% end %>
    </div>
    """
  end

  attr :ferry, :map, required: true
  attr :stats, :map, required: true
  attr :selected, :boolean, default: false

  defp ferry_card(assigns) do
    is_paused = assigns.stats.status == :paused

    assigns =
      assigns
      |> assign(:is_paused, is_paused)
      |> assign(:success_rate, success_rate(assigns.stats))
      |> assign(:color_classes, color_classes(assigns.ferry.color))

    ~H"""
    <div
      phx-click="select_ferry"
      phx-value-ferry={@ferry.module}
      class={"group relative rounded-xl border transition-all duration-300 cursor-pointer hover:scale-[1.02] " <>
        if(@selected,
          do: @color_classes.selected_border,
          else: "border-zinc-800/50 bg-zinc-900/30 hover:border-zinc-700/50 hover:bg-zinc-900/50"
        )}
    >
      <!-- Glow effect -->
      <div class={"absolute inset-0 rounded-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500 " <> @color_classes.glow} />

      <div class="relative p-5">
        <!-- Header -->
        <div class="flex items-start justify-between mb-4">
          <div class="flex items-center gap-2.5">
            <div class={"w-8 h-8 rounded-lg flex items-center justify-center " <> @color_classes.icon_bg}>
              <.ferry_icon name={@ferry.icon} class={"w-4 h-4 " <> @color_classes.queue_text} />
            </div>
            <div>
              <h3 class="text-sm font-medium text-zinc-200">{@ferry.label}</h3>
              <p class="text-[10px] text-zinc-600 mt-0.5">{@ferry.description}</p>
            </div>
          </div>
          <div class={"px-2 py-0.5 rounded-full text-[10px] font-medium " <>
            if(@is_paused,
              do: "bg-amber-500/10 text-amber-400 border border-amber-500/20",
              else: "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
            )}>
            {if @is_paused, do: "Paused", else: "Running"}
          </div>
        </div>
        
    <!-- Stats Grid -->
        <div class="grid grid-cols-3 gap-3 mb-4">
          <div>
            <p class="text-[10px] text-zinc-600 uppercase tracking-wider">Queue</p>
            <p class={"text-lg font-mono font-semibold transition-all duration-300 " <> @color_classes.queue_text}>
              {@stats.queue_size}
            </p>
          </div>
          <div>
            <p class="text-[10px] text-zinc-600 uppercase tracking-wider">Done</p>
            <p class="text-lg font-mono font-semibold text-zinc-300 transition-all duration-300">
              {format_number(@stats.total_processed)}
            </p>
          </div>
          <div>
            <p class="text-[10px] text-zinc-600 uppercase tracking-wider">DLQ</p>
            <p class={"text-lg font-mono font-semibold transition-all duration-300 " <>
              if(@stats.dlq_size > 0, do: "text-red-400", else: "text-zinc-600")}>
              {@stats.dlq_size}
            </p>
          </div>
        </div>
        
    <!-- Progress Bar -->
        <div class="mb-4">
          <div class="flex justify-between text-[10px] text-zinc-600 mb-1">
            <span>Success rate</span>
            <span class="font-mono">{@success_rate}%</span>
          </div>
          <div class="h-1.5 bg-zinc-800 rounded-full overflow-hidden">
            <div
              class={"h-full rounded-full transition-all duration-700 ease-out bg-gradient-to-r " <>
                cond do
                  @success_rate >= 95 -> "from-emerald-500 to-emerald-400"
                  @success_rate >= 80 -> "from-amber-500 to-amber-400"
                  true -> "from-red-500 to-red-400"
                end}
              style={"width: #{@success_rate}%"}
            />
          </div>
        </div>
        
    <!-- Ferry Config -->
        <div class="mb-4 grid grid-cols-4 gap-2">
          <div class="px-2 py-1.5 rounded-lg bg-zinc-800/40 border border-zinc-700/20">
            <p class="text-[9px] text-zinc-600 uppercase tracking-wider">Batch</p>
            <p class="text-xs font-mono font-medium text-zinc-400">{@ferry.config.batch_size} ops</p>
          </div>
          <div class="px-2 py-1.5 rounded-lg bg-zinc-800/40 border border-zinc-700/20">
            <p class="text-[9px] text-zinc-600 uppercase tracking-wider">Flush</p>
            <p class="text-xs font-mono font-medium text-zinc-400">{format_interval(@ferry.config.flush_interval)}</p>
          </div>
          <div class="px-2 py-1.5 rounded-lg bg-zinc-800/40 border border-zinc-700/20">
            <p class="text-[9px] text-zinc-600 uppercase tracking-wider">Max queue</p>
            <p class="text-xs font-mono font-medium text-zinc-400">{format_number(@ferry.config.max_queue_size)}</p>
          </div>
          <div class="px-2 py-1.5 rounded-lg bg-zinc-800/40 border border-zinc-700/20">
            <p class="text-[9px] text-zinc-600 uppercase tracking-wider">Timeout</p>
            <p class="text-xs font-mono font-medium text-zinc-400">{format_interval(@ferry.config.operation_timeout)}</p>
          </div>
        </div>

    <!-- Action Buttons -->
        <div class="flex gap-1.5">
          <button
            phx-click="push"
            phx-value-ferry={@ferry.module}
            phx-value-count="1"
            class="flex-1 px-2.5 py-1.5 rounded-lg bg-zinc-800/50 hover:bg-zinc-700/50 border border-zinc-700/30 text-[11px] text-zinc-400 hover:text-zinc-200 transition-all duration-200 active:scale-95"
          >
            +1
          </button>
          <button
            phx-click="push"
            phx-value-ferry={@ferry.module}
            phx-value-count="10"
            class="flex-1 px-2.5 py-1.5 rounded-lg bg-zinc-800/50 hover:bg-zinc-700/50 border border-zinc-700/30 text-[11px] text-zinc-400 hover:text-zinc-200 transition-all duration-200 active:scale-95"
          >
            +10
          </button>
          <button
            phx-click="push"
            phx-value-ferry={@ferry.module}
            phx-value-count="50"
            class="flex-1 px-2.5 py-1.5 rounded-lg bg-zinc-800/50 hover:bg-zinc-700/50 border border-zinc-700/30 text-[11px] text-zinc-400 hover:text-zinc-200 transition-all duration-200 active:scale-95"
          >
            +50
          </button>
          <button
            phx-click="flush"
            phx-value-ferry={@ferry.module}
            class="px-2.5 py-1.5 rounded-lg bg-indigo-500/10 hover:bg-indigo-500/20 border border-indigo-500/20 text-[11px] text-indigo-400 hover:text-indigo-300 transition-all duration-200 active:scale-95"
          >
            <svg class="w-3.5 h-3.5 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
            </svg>
            Flush
          </button>
        </div>
      </div>
    </div>
    """
  end

  defp ferry_detail(assigns) do
    ~H"""
    <div class="rounded-xl border border-zinc-800/50 bg-zinc-900/30 overflow-hidden">
      <!-- Detail Header -->
      <div class="px-5 py-4 border-b border-zinc-800/50 flex items-center justify-between">
        <div class="flex items-center gap-3">
          <.ferry_icon name={@ferry.icon} class="w-5 h-5 text-zinc-400" />
          <div>
            <h2 class="text-sm font-medium">{@ferry.label}</h2>
            <p class="text-[10px] text-zinc-600 font-mono">{inspect(@ferry.module)}</p>
          </div>
        </div>
        <div class="flex items-center gap-2">
          <%= if @stats.status == :paused do %>
            <button
              phx-click="resume"
              phx-value-ferry={@ferry.module}
              class="px-3 py-1.5 rounded-lg bg-emerald-500/10 hover:bg-emerald-500/20 border border-emerald-500/20 text-xs text-emerald-400 transition-all duration-200"
            >
              <svg class="w-3.5 h-3.5 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="currentColor" stroke="none">
                <polygon points="5 3 19 12 5 21 5 3" />
              </svg>
              Resume
            </button>
          <% else %>
            <button
              phx-click="pause"
              phx-value-ferry={@ferry.module}
              class="px-3 py-1.5 rounded-lg bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/20 text-xs text-amber-400 transition-all duration-200"
            >
              <svg class="w-3.5 h-3.5 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <rect x="6" y="4" width="4" height="16" rx="1" />
                <rect x="14" y="4" width="4" height="16" rx="1" />
              </svg>
              Pause
            </button>
          <% end %>
        </div>
      </div>
      
    <!-- Stats Row -->
      <div class="px-5 py-3 grid grid-cols-6 gap-4 border-b border-zinc-800/30 bg-zinc-900/50">
        <.stat_pill label="Pushed" value={@stats.total_pushed} />
        <.stat_pill label="Processed" value={@stats.total_processed} color="emerald" />
        <.stat_pill label="Failed" value={@stats.total_failed} color="red" />
        <.stat_pill label="Rejected" value={@stats.total_rejected} color="amber" />
        <.stat_pill label="Batches" value={@stats.batches_executed} color="blue" />
        <.stat_pill
          label="Avg ms"
          value={Float.round(@stats.avg_batch_duration_ms, 1)}
          color="violet"
        />
      </div>
      
    <!-- Tabs -->
      <div class="flex border-b border-zinc-800/30">
        <.detail_tab_button
          label="Operations"
          tab={:operations}
          active={@detail_tab}
          count={length(@pending_ops)}
          count_color="text-amber-400"
        />
        <.detail_tab_button
          label="Activity"
          tab={:activity}
          active={@detail_tab}
          count={length(@ferry_events)}
          count_color="text-indigo-400"
        />
        <.detail_tab_button
          label="Completed"
          tab={:completed}
          active={@detail_tab}
          count={length(@completed_ops)}
          count_color="text-emerald-400"
        />
        <.detail_tab_button
          label="DLQ"
          tab={:dlq}
          active={@detail_tab}
          count={@stats.dlq_size}
          count_color="text-red-400"
        />
      </div>
      
    <!-- Tab Content -->
      <div class="max-h-[420px] overflow-y-auto">
        <%= case @detail_tab do %>
          <% :operations -> %>
            <.pending_panel pending={@pending_ops} new_ids={@new_pending_ids} />
          <% :activity -> %>
            <.activity_panel events={@ferry_events} />
          <% :completed -> %>
            <.completed_panel completed={@completed_ops} new_ids={@new_completed_ids} />
          <% :dlq -> %>
            <.dlq_panel dead_letters={@dead_letters} ferry={@ferry} new_ids={@new_dead_ids} />
        <% end %>
      </div>
    </div>
    """
  end

  defp detail_tab_button(assigns) do
    active = assigns.tab == assigns.active

    assigns = assign(assigns, :is_active, active)

    ~H"""
    <button
      phx-click="detail_tab"
      phx-value-tab={@tab}
      class={"flex-1 px-4 py-2.5 text-[11px] font-medium transition-all duration-200 border-b-2 " <>
        if(@is_active,
          do: "text-zinc-200 border-zinc-400",
          else: "text-zinc-600 border-transparent hover:text-zinc-400 hover:border-zinc-700"
        )}
    >
      {@label}
      <span class={"ml-1.5 font-mono " <> if(@is_active, do: @count_color, else: "text-zinc-700")}>
        {@count}
      </span>
    </button>
    """
  end

  defp stat_pill(assigns) do
    color = Map.get(assigns, :color, "zinc")
    assigns = assign(assigns, :text_class, stat_text_class(color))

    ~H"""
    <div>
      <p class="text-[10px] text-zinc-600 uppercase tracking-wider">{@label}</p>
      <p class={"text-sm font-mono font-semibold " <> @text_class}>{@value}</p>
    </div>
    """
  end

  defp stat_text_class("emerald"), do: "text-emerald-400"
  defp stat_text_class("red"), do: "text-red-400"
  defp stat_text_class("amber"), do: "text-amber-400"
  defp stat_text_class("blue"), do: "text-blue-400"
  defp stat_text_class("violet"), do: "text-violet-400"
  defp stat_text_class(_), do: "text-zinc-400"

  defp pending_panel(assigns) do
    ~H"""
    <div class="px-5 py-3">
      <%= if length(@pending) > 0 do %>
        <div class="space-y-1">
          <div
            :for={op <- @pending}
            id={"pending-#{op.id}"}
            class={"flex items-center justify-between px-3 py-2 rounded-lg bg-zinc-800/30 text-xs" <>
              if(MapSet.member?(@new_ids, op.id), do: " animate-flash-new", else: "")}
          >
            <div class="flex items-center gap-2.5">
              <span class="relative flex h-2 w-2">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-60">
                </span>
                <span class="relative inline-flex rounded-full h-2 w-2 bg-amber-400"></span>
              </span>
              <span class="font-mono text-zinc-400">{String.slice(op.id, 0..13)}</span>
            </div>
            <div class="flex items-center gap-3">
              <span class="text-[10px] text-zinc-600 font-mono truncate max-w-[180px]">
                {inspect_payload(op.payload)}
              </span>
              <span class="text-zinc-700 text-[10px] font-mono">#{op.order}</span>
            </div>
          </div>
        </div>
      <% else %>
        <div class="py-8 text-center text-zinc-700 text-xs">
          Queue empty — push operations or enable simulator
        </div>
      <% end %>
    </div>
    """
  end

  defp activity_panel(assigns) do
    ~H"""
    <div class="divide-y divide-zinc-800/20">
      <%= if length(@events) > 0 do %>
        <div
          :for={event <- @events}
          class="px-5 py-2 hover:bg-zinc-800/20 transition-colors duration-100 animate-slide-in"
        >
          <div class="flex items-start gap-2.5">
            <span class={"mt-1 w-2 h-2 rounded-full shrink-0 " <> activity_dot(event.event)} />
            <div class="min-w-0 flex-1">
              <div class="flex items-center justify-between gap-2">
                <p class="text-[11px] font-medium text-zinc-300">
                  {activity_label(event.event)}
                </p>
                <span class="text-[9px] text-zinc-700 font-mono shrink-0">
                  {Calendar.strftime(event.timestamp, "%H:%M:%S.") <>
                    String.slice(to_string(event.timestamp.microsecond |> elem(0)), 0..2)}
                </span>
              </div>
              <p class="text-[10px] text-zinc-500 mt-0.5">
                {activity_detail(event)}
              </p>
            </div>
          </div>
        </div>
      <% else %>
        <div class="px-5 py-8 text-center text-zinc-700 text-xs">
          No activity yet — events will appear here in real-time
        </div>
      <% end %>
    </div>
    """
  end

  defp completed_panel(assigns) do
    ~H"""
    <div class="px-5 py-3">
      <%= if length(@completed) > 0 do %>
        <div class="space-y-1.5">
          <div
            :for={op <- @completed}
            id={"completed-#{op.id}"}
            class={"px-3 py-2.5 rounded-lg bg-zinc-800/20 text-xs" <>
              if(MapSet.member?(@new_ids, op.id), do: " animate-flash-new", else: "")}
          >
            <div class="flex items-center justify-between mb-1.5">
              <div class="flex items-center gap-2.5">
                <span class="w-2 h-2 rounded-full bg-emerald-400" />
                <span class="font-mono text-zinc-400">{String.slice(op.id, 0..13)}</span>
              </div>
              <span class="text-zinc-700 text-[10px] font-mono">
                {if op.completed_at, do: Calendar.strftime(op.completed_at, "%H:%M:%S"), else: "—"}
              </span>
            </div>
            <div class="pl-4.5 space-y-0.5">
              <p class="text-[10px] text-zinc-500 font-mono truncate">
                <span class="text-zinc-600">in:</span> {inspect_payload(op.payload)}
              </p>
              <p class="text-[10px] text-emerald-400/70 font-mono truncate">
                <span class="text-zinc-600">out:</span> {inspect_payload(op.result)}
              </p>
            </div>
          </div>
        </div>
      <% else %>
        <div class="py-8 text-center text-zinc-700 text-xs">
          No completed operations yet
        </div>
      <% end %>
    </div>
    """
  end

  defp dlq_panel(assigns) do
    ~H"""
    <div class="px-5 py-3">
      <div class="flex items-center justify-between mb-3">
        <p class="text-[10px] text-zinc-600 uppercase tracking-wider">
          Dead Letters ({length(@dead_letters)})
        </p>
        <%= if length(@dead_letters) > 0 do %>
          <div class="flex gap-1.5">
            <button
              phx-click="retry_dlq"
              phx-value-ferry={@ferry.module}
              class="px-2.5 py-1 rounded-md bg-indigo-500/10 hover:bg-indigo-500/20 border border-indigo-500/20 text-[10px] text-indigo-400 transition-all duration-200"
            >
              <svg class="w-3 h-3 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <polyline points="23 4 23 10 17 10" />
                <path d="M20.49 15a9 9 0 11-2.12-9.36L23 10" />
              </svg>
              Retry All
            </button>
            <button
              phx-click="drain_dlq"
              phx-value-ferry={@ferry.module}
              class="px-2.5 py-1 rounded-md bg-red-500/10 hover:bg-red-500/20 border border-red-500/20 text-[10px] text-red-400 transition-all duration-200"
            >
              <svg class="w-3 h-3 inline-block mr-0.5 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <polyline points="3 6 5 6 21 6" />
                <path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
                <line x1="10" y1="11" x2="10" y2="17" />
                <line x1="14" y1="11" x2="14" y2="17" />
              </svg>
              Drain
            </button>
          </div>
        <% end %>
      </div>

      <%= if length(@dead_letters) == 0 do %>
        <div class="py-6 text-center text-zinc-600 text-xs">
          No dead letters — everything is healthy
            <svg class="w-3.5 h-3.5 inline-block ml-1 -mt-0.5 text-emerald-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polyline points="20 6 9 17 4 12" />
            </svg>
        </div>
      <% else %>
        <div class="space-y-1.5">
          <div
            :for={op <- @dead_letters}
            id={"dlq-#{op.id}"}
            class={"px-3 py-2.5 rounded-lg bg-red-950/20 border border-red-900/20 text-xs" <>
              if(MapSet.member?(@new_ids, op.id), do: " animate-flash-new", else: "")}
          >
            <div class="flex items-center justify-between mb-1">
              <div class="flex items-center gap-2">
                <span class="w-1.5 h-1.5 rounded-full bg-red-400" />
                <span class="font-mono text-zinc-400">{String.slice(op.id, 0..13)}</span>
              </div>
              <span class="text-red-400/60 text-[10px]">#{op.order}</span>
            </div>
            <div class="pl-3.5 space-y-0.5">
              <p class="text-[10px] text-zinc-500 font-mono truncate">
                <span class="text-zinc-600">payload:</span> {inspect_payload(op.payload)}
              </p>
              <p class="text-[10px] text-red-400/80 font-mono truncate">
                <span class="text-zinc-600">error:</span>
                {inspect(op.error, pretty: false, limit: 50)}
              </p>
            </div>
          </div>
        </div>
      <% end %>
    </div>
    """
  end

  defp event_feed(assigns) do
    ~H"""
    <div class="rounded-xl border border-zinc-800/50 bg-zinc-900/30 overflow-hidden">
      <div class="px-5 py-3 border-b border-zinc-800/50 flex items-center justify-between">
        <div class="flex items-center gap-2">
          <span class="relative flex h-2 w-2">
            <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75">
            </span>
            <span class="relative inline-flex rounded-full h-2 w-2 bg-indigo-500"></span>
          </span>
          <h3 class="text-xs font-medium text-zinc-400">Live Event Feed</h3>
        </div>
        <span class="text-[10px] text-zinc-600 font-mono">{length(@events)} events</span>
      </div>

      <div class="max-h-[580px] overflow-y-auto divide-y divide-zinc-800/20">
        <%= if length(@events) == 0 do %>
          <div class="px-5 py-8 text-center text-zinc-600 text-xs">
            Waiting for events...
          </div>
        <% else %>
          <div
            :for={event <- Enum.take(@events, 40)}
            class="px-4 py-2 hover:bg-zinc-800/20 transition-colors duration-150 animate-slide-in"
          >
            <div class="flex items-start gap-2">
              <span class={"mt-1 w-1.5 h-1.5 rounded-full shrink-0 " <> event_dot_color(event.event)} />
              <div class="min-w-0 flex-1">
                <div class="flex items-center justify-between gap-2">
                  <p class="text-[11px] font-medium text-zinc-400 truncate">
                    {event_label(event.event)}
                  </p>
                  <span class="text-[9px] text-zinc-700 font-mono shrink-0">
                    {Calendar.strftime(event.timestamp, "%H:%M:%S")}
                  </span>
                </div>
                <p class="text-[10px] text-zinc-600 truncate">
                  {event_detail(event)}
                </p>
              </div>
            </div>
          </div>
        <% end %>
      </div>
    </div>
    """
  end

  # ── Helpers ──

  defp filter_events_for(events, ferry_module) do
    Enum.filter(events, fn %{metadata: meta} ->
      Map.get(meta, :ferry) == ferry_module
    end)
    |> Enum.take(80)
  end

  defp inspect_payload(val) when is_map(val) do
    val
    |> Enum.take(3)
    |> Enum.map_join(", ", fn {k, v} -> "#{k}: #{inspect(v, limit: 20)}" end)
  end

  defp inspect_payload(val), do: inspect(val, limit: 30, pretty: false)

  defp activity_dot([:ferry, :operation, :pushed]), do: "bg-blue-400"
  defp activity_dot([:ferry, :operation, :completed]), do: "bg-emerald-400"
  defp activity_dot([:ferry, :operation, :failed]), do: "bg-red-400"
  defp activity_dot([:ferry, :operation, :dead_lettered]), do: "bg-red-500"
  defp activity_dot([:ferry, :batch, :started]), do: "bg-indigo-400"
  defp activity_dot([:ferry, :batch, :completed]), do: "bg-cyan-400"
  defp activity_dot([:ferry, :batch, :timeout]), do: "bg-red-500"
  defp activity_dot(_), do: "bg-zinc-500"

  defp activity_label([:ferry, :operation, :pushed]), do: "Pushed"
  defp activity_label([:ferry, :operation, :completed]), do: "Completed"
  defp activity_label([:ferry, :operation, :failed]), do: "Failed"
  defp activity_label([:ferry, :operation, :dead_lettered]), do: "Dead lettered"
  defp activity_label([:ferry, :batch, :started]), do: "Batch started"
  defp activity_label([:ferry, :batch, :completed]), do: "Batch done"
  defp activity_label([:ferry, :batch, :timeout]), do: "Batch timeout"
  defp activity_label([:ferry, :operation, :rejected]), do: "Rejected"
  defp activity_label(event), do: event |> List.last() |> to_string() |> String.capitalize()

  defp activity_detail(%{event: [:ferry, :batch, :completed], measurements: m}) do
    "#{m.succeeded} succeeded, #{m.failed} failed — #{trunc(m.duration_ms)}ms"
  end

  defp activity_detail(%{event: [:ferry, :batch, :started], measurements: m}) do
    "#{m.batch_size} ops dequeued, #{m.queue_size_before} were in queue"
  end

  defp activity_detail(%{event: [:ferry, :operation, :failed], metadata: m}) do
    "#{String.slice(m.operation_id, 0..13)} — #{inspect(Map.get(m, :error, ""))}"
  end

  defp activity_detail(%{event: [:ferry, :operation, _], metadata: m}) do
    String.slice(Map.get(m, :operation_id, ""), 0..17)
  end

  defp activity_detail(_), do: ""

  defp success_rate(%{total_processed: 0, total_failed: 0}), do: 100.0

  defp success_rate(%{total_processed: p, total_failed: f}) do
    total = p + f
    if total > 0, do: Float.round(p / total * 100, 1), else: 100.0
  end

  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}k"
  defp format_number(n), do: "#{n}"

  defp format_interval(ms) when ms >= 60_000, do: "#{div(ms, 60_000)}m"
  defp format_interval(ms) when ms >= 1_000, do: "#{div(ms, 1_000)}s"
  defp format_interval(ms), do: "#{ms}ms"

  defp event_label([:ferry, :operation, :pushed]), do: "Operation pushed"
  defp event_label([:ferry, :operation, :completed]), do: "Operation completed"
  defp event_label([:ferry, :operation, :failed]), do: "Operation failed"
  defp event_label([:ferry, :operation, :dead_lettered]), do: "Dead lettered"
  defp event_label([:ferry, :operation, :rejected]), do: "Rejected (queue full)"
  defp event_label([:ferry, :batch, :started]), do: "Batch started"
  defp event_label([:ferry, :batch, :completed]), do: "Batch completed"
  defp event_label([:ferry, :batch, :timeout]), do: "Batch timeout"
  defp event_label([:ferry, :queue, :paused]), do: "Queue paused"
  defp event_label([:ferry, :queue, :resumed]), do: "Queue resumed"
  defp event_label([:ferry, :dlq, :retried]), do: "DLQ retried"
  defp event_label([:ferry, :dlq, :drained]), do: "DLQ drained"
  defp event_label([:ferry, :completed, :purged]), do: "Completed purged"
  defp event_label(event), do: Enum.join(event, ".")

  defp event_dot_color([:ferry, :operation, :completed]), do: "bg-emerald-400"
  defp event_dot_color([:ferry, :operation, :pushed]), do: "bg-blue-400"
  defp event_dot_color([:ferry, :operation, :failed]), do: "bg-red-400"
  defp event_dot_color([:ferry, :operation, :dead_lettered]), do: "bg-red-500"
  defp event_dot_color([:ferry, :operation, :rejected]), do: "bg-amber-400"
  defp event_dot_color([:ferry, :batch, :started]), do: "bg-indigo-400"
  defp event_dot_color([:ferry, :batch, :completed]), do: "bg-indigo-300"
  defp event_dot_color([:ferry, :batch, :timeout]), do: "bg-red-500"
  defp event_dot_color([:ferry, :queue, :paused]), do: "bg-amber-400"
  defp event_dot_color([:ferry, :queue, :resumed]), do: "bg-emerald-400"
  defp event_dot_color([:ferry, :dlq | _]), do: "bg-orange-400"
  defp event_dot_color(_), do: "bg-zinc-500"

  defp event_detail(%{event: [:ferry, :batch, :completed], measurements: m, metadata: meta}) do
    ferry_short = meta.ferry |> inspect() |> String.split(".") |> List.last()
    "#{ferry_short} · #{m.succeeded} ok, #{m.failed} failed · #{trunc(m.duration_ms)}ms"
  end

  defp event_detail(%{event: [:ferry, :batch, :started], measurements: m, metadata: meta}) do
    ferry_short = meta.ferry |> inspect() |> String.split(".") |> List.last()
    "#{ferry_short} · #{m.batch_size} ops · #{m.queue_size_before} in queue"
  end

  defp event_detail(%{event: [:ferry, :operation, _], metadata: meta}) do
    ferry_short = meta.ferry |> inspect() |> String.split(".") |> List.last()
    op_id = Map.get(meta, :operation_id, "")
    "#{ferry_short} · #{String.slice(op_id, 0..13)}"
  end

  defp event_detail(%{metadata: meta}) do
    ferry_short = meta.ferry |> inspect() |> String.split(".") |> List.last()
    "#{ferry_short}"
  end

  # Static color class maps — ensures Tailwind v4 can detect all classes at build time
  defp color_classes("amber") do
    %{
      selected_border: "border-amber-500/40 bg-amber-950/20 shadow-lg shadow-amber-500/5",
      glow: "bg-gradient-to-br from-amber-500/5 to-transparent",
      queue_text: "text-amber-400",
      icon_bg: "bg-amber-500/10"
    }
  end

  defp color_classes("blue") do
    %{
      selected_border: "border-blue-500/40 bg-blue-950/20 shadow-lg shadow-blue-500/5",
      glow: "bg-gradient-to-br from-blue-500/5 to-transparent",
      queue_text: "text-blue-400",
      icon_bg: "bg-blue-500/10"
    }
  end

  defp color_classes("emerald") do
    %{
      selected_border: "border-emerald-500/40 bg-emerald-950/20 shadow-lg shadow-emerald-500/5",
      glow: "bg-gradient-to-br from-emerald-500/5 to-transparent",
      queue_text: "text-emerald-400",
      icon_bg: "bg-emerald-500/10"
    }
  end

  defp color_classes("violet") do
    %{
      selected_border: "border-violet-500/40 bg-violet-950/20 shadow-lg shadow-violet-500/5",
      glow: "bg-gradient-to-br from-violet-500/5 to-transparent",
      queue_text: "text-violet-400",
      icon_bg: "bg-violet-500/10"
    }
  end

  defp color_classes(_) do
    %{
      selected_border: "border-zinc-500/40 bg-zinc-950/20 shadow-lg shadow-zinc-500/5",
      glow: "bg-gradient-to-br from-zinc-500/5 to-transparent",
      queue_text: "text-zinc-400",
      icon_bg: "bg-zinc-500/10"
    }
  end

  # ── SVG Icon components ──

  attr :name, :atom, required: true
  attr :class, :string, default: "w-5 h-5"

  defp ferry_icon(%{name: :inventory} = assigns) do
    ~H"""
    <svg
      class={@class}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    >
      <path d="M21 8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" />
      <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
      <line x1="12" y1="22.08" x2="12" y2="12" />
    </svg>
    """
  end

  defp ferry_icon(%{name: :email} = assigns) do
    ~H"""
    <svg
      class={@class}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    >
      <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z" />
      <path d="M22 6l-10 7L2 6" />
    </svg>
    """
  end

  defp ferry_icon(%{name: :positions} = assigns) do
    ~H"""
    <svg
      class={@class}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    >
      <circle cx="12" cy="12" r="10" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z" />
      <circle cx="12" cy="12" r="2" fill="currentColor" stroke="none" />
    </svg>
    """
  end

  defp ferry_icon(%{name: :ai_jobs} = assigns) do
    ~H"""
    <svg
      class={@class}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    >
      <path d="M12 2L2 7l10 5 10-5-10-5z" />
      <path d="M2 17l10 5 10-5" />
      <path d="M2 12l10 5 10-5" />
    </svg>
    """
  end

  defp ferry_icon(assigns) do
    ~H"""
    <svg
      class={@class}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    >
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="8" x2="12" y2="12" />
      <line x1="12" y1="16" x2="12.01" y2="16" />
    </svg>
    """
  end
end
