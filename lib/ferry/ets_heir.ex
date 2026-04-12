defmodule Ferry.EtsHeir do
  @moduledoc """
  Heir process for ETS tables.

  When the Ferry.Server crashes, ETS tables are transferred to this process.
  When the server restarts, it requests the tables back.
  """

  use GenServer

  def start_link(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)
    GenServer.start_link(__MODULE__, opts, name: via(ferry_name))
  end

  def register_server(ferry_name, server_pid) do
    GenServer.call(via(ferry_name), {:register_server, server_pid})
  end

  # Callbacks

  @impl true
  def init(opts) do
    ferry_name = Keyword.fetch!(opts, :ferry_name)

    state = %{
      ferry_name: ferry_name,
      server_pid: nil,
      holding_tables: []
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:register_server, server_pid}, _from, state) do
    Process.monitor(server_pid)

    # If we're holding tables, give them back
    Enum.each(state.holding_tables, fn table ->
      if :ets.whereis(table) != :undefined do
        try do
          :ets.give_away(table, server_pid, {:heir_handback, state.ferry_name})
        rescue
          ArgumentError -> :ok
        end
      end
    end)

    # Set ourselves as heir on all tables
    Ferry.Store.Ets.set_heir(state.ferry_name, self())

    {:reply, :ok, %{state | server_pid: server_pid, holding_tables: []}}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", table, _from_pid, {_ferry_name, _type}}, state) do
    # Server crashed, we're inheriting the table
    {:noreply, %{state | holding_tables: [table | state.holding_tables]}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{server_pid: pid} = state) do
    # Server went down — we hold onto tables until it restarts
    {:noreply, %{state | server_pid: nil}}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp via(ferry_name) do
    :"#{ferry_name}.EtsHeir"
  end
end
