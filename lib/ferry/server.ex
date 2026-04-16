defmodule Ferry.Server do
  @moduledoc """
  Core GenServer that manages the operation queue, flush cycle, and resolver execution.
  """

  use GenServer

  require Logger

  alias Ferry.{Operation, Batch, IdGenerator, Telemetry}

  @default_config %{
    batch_size: 10,
    flush_interval: 5_000,
    max_queue_size: 10_000,
    auto_flush: true,
    operation_timeout: 300_000,
    max_completed: 1_000,
    completed_ttl: :timer.minutes(30),
    persistence: :memory,
    id_generator: &IdGenerator.generate/1
  }

  @purge_interval :timer.seconds(30)

  # Client API

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  def push(name, payload) do
    GenServer.call(via(name), {:push, payload})
  end

  def push_many(name, payloads) do
    GenServer.call(via(name), {:push_many, payloads})
  end

  def status(name, operation_id) do
    GenServer.call(via(name), {:status, operation_id})
  end

  def stats(name) do
    GenServer.call(via(name), :stats)
  end

  def queue_size(name) do
    GenServer.call(via(name), :queue_size)
  end

  def flush(name) do
    GenServer.call(via(name), {:flush, :manual}, :infinity)
  end

  def pause(name) do
    GenServer.call(via(name), :pause)
  end

  def resume(name) do
    GenServer.call(via(name), :resume)
  end

  def dead_letters(name) do
    GenServer.call(via(name), :dead_letters)
  end

  def dead_letter_count(name) do
    GenServer.call(via(name), :dead_letter_count)
  end

  def retry_dead_letters(name) do
    GenServer.call(via(name), :retry_dead_letters)
  end

  def drain_dead_letters(name) do
    GenServer.call(via(name), :drain_dead_letters)
  end

  def status_many(name, operation_ids) do
    GenServer.call(via(name), {:status_many, operation_ids})
  end

  def clear(name) do
    GenServer.call(via(name), :clear)
  end

  def pending(name, opts \\ []) do
    GenServer.call(via(name), {:pending, opts})
  end

  def completed(name, opts \\ []) do
    GenServer.call(via(name), {:completed, opts})
  end

  def config(name) do
    GenServer.call(via(name), :config)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    resolver = Keyword.fetch!(opts, :resolver)

    config =
      @default_config
      |> Map.merge(Map.new(Keyword.take(opts, Map.keys(@default_config))))
      |> Map.put(:name, name)
      |> Map.put(:resolver, resolver)

    store_module = store_module(config.persistence)
    {:ok, store_state} = store_module.init(name, opts)

    state = %{
      config: config,
      store_module: store_module,
      store: store_state,
      order_counter: 0,
      timer_ref: nil,
      status: :running,
      started_at: DateTime.utc_now(),
      total_pushed: 0,
      total_processed: 0,
      total_failed: 0,
      total_rejected: 0,
      batches_executed: 0,
      total_batch_duration_ms: 0.0,
      last_flush_at: nil,
      # Tracks in-flight async batch execution
      flushing: nil,
      flush_callers: []
    }

    # Register with ETS heir if using ETS persistence
    if config.persistence == :ets do
      try do
        Ferry.EtsHeir.register_server(name, self())
      rescue
        _ -> :ok
      end
    end

    # Start auto-flush timer
    state =
      if config.auto_flush do
        schedule_flush(state)
      else
        state
      end

    # Start purge timer
    schedule_purge()

    Logger.debug(
      "[Ferry:#{name}] Started with config: batch_size=#{config.batch_size}, flush_interval=#{config.flush_interval}ms, max_queue_size=#{config.max_queue_size}"
    )

    {:ok, state}
  end

  @impl true
  def handle_call({:push, payload}, _from, state) do
    queue_size = state.store_module.queue_size(state.store)

    if queue_size >= state.config.max_queue_size do
      Telemetry.emit_rejected(state.config.name, queue_size)
      {:reply, {:error, :queue_full}, %{state | total_rejected: state.total_rejected + 1}}
    else
      {id, state} = generate_id(state, payload)
      op = build_operation(id, payload, state.order_counter)

      {:ok, new_store} = state.store_module.push(state.store, op)
      new_queue_size = queue_size + 1

      Telemetry.emit_pushed(state.config.name, id, new_queue_size)

      {:reply, {:ok, id},
       %{
         state
         | store: new_store,
           order_counter: state.order_counter + 1,
           total_pushed: state.total_pushed + 1
       }}
    end
  end

  @impl true
  def handle_call({:push_many, payloads}, _from, state) do
    queue_size = state.store_module.queue_size(state.store)
    needed = length(payloads)

    if queue_size + needed > state.config.max_queue_size do
      Telemetry.emit_rejected(state.config.name, queue_size)
      {:reply, {:error, :queue_full}, %{state | total_rejected: state.total_rejected + 1}}
    else
      {ops, state} =
        Enum.map_reduce(payloads, state, fn payload, acc ->
          {id, acc} = generate_id(acc, payload)
          op = build_operation(id, payload, acc.order_counter)
          {op, %{acc | order_counter: acc.order_counter + 1}}
        end)

      {:ok, new_store} = state.store_module.push_many(state.store, ops)
      ids = Enum.map(ops, & &1.id)

      Enum.each(ids, fn id ->
        Telemetry.emit_pushed(state.config.name, id, queue_size + needed)
      end)

      {:reply, {:ok, ids}, %{state | store: new_store, total_pushed: state.total_pushed + needed}}
    end
  end

  @impl true
  def handle_call({:flush, trigger}, from, state) do
    queue_size = state.store_module.queue_size(state.store)

    if queue_size == 0 do
      {:reply, {:error, :empty_queue}, state}
    else
      if state.flushing != nil do
        # Already flushing — queue caller to be notified when done
        {:noreply, %{state | flush_callers: [from | state.flush_callers]}}
      else
        state = start_async_flush(state, trigger)
        {:noreply, %{state | flush_callers: [from | state.flush_callers]}}
      end
    end
  end

  @impl true
  def handle_call({:status, operation_id}, _from, state) do
    result = state.store_module.get(state.store, operation_id)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    stats = build_stats(state)
    {:reply, stats, state}
  end

  @impl true
  def handle_call(:queue_size, _from, state) do
    size = state.store_module.queue_size(state.store)
    {:reply, size, state}
  end

  @impl true
  def handle_call(:config, _from, state) do
    config =
      Map.take(state.config, [
        :batch_size,
        :flush_interval,
        :max_queue_size,
        :operation_timeout,
        :max_completed,
        :completed_ttl,
        :persistence,
        :auto_flush
      ])

    {:reply, config, state}
  end

  @impl true
  def handle_call(:pause, _from, state) do
    state = cancel_timer(state)
    Telemetry.emit_paused(state.config.name)
    Logger.info("[Ferry:#{state.config.name}] Paused")
    {:reply, :ok, %{state | status: :paused}}
  end

  @impl true
  def handle_call(:resume, _from, state) do
    state = schedule_flush(state)
    Telemetry.emit_resumed(state.config.name)
    Logger.info("[Ferry:#{state.config.name}] Resumed")
    {:reply, :ok, %{state | status: :running}}
  end

  @impl true
  def handle_call(:dead_letters, _from, state) do
    ops = state.store_module.list_dlq(state.store)
    {:reply, ops, state}
  end

  @impl true
  def handle_call(:dead_letter_count, _from, state) do
    count = state.store_module.dlq_size(state.store)
    {:reply, count, state}
  end

  @impl true
  def handle_call(:retry_dead_letters, _from, state) do
    {count, new_store} = state.store_module.retry_all_dlq(state.store)
    Telemetry.emit_dlq_retried(state.config.name, count)
    Logger.info("[Ferry:#{state.config.name}] Retried #{count} dead letters")
    {:reply, {:ok, count}, %{state | store: new_store}}
  end

  @impl true
  def handle_call(:drain_dead_letters, _from, state) do
    {count, new_store} = state.store_module.drain_dlq(state.store)
    Telemetry.emit_dlq_drained(state.config.name, count)
    Logger.info("[Ferry:#{state.config.name}] Drained #{count} dead letters")
    {:reply, :ok, %{state | store: new_store}}
  end

  @impl true
  def handle_call({:status_many, operation_ids}, _from, state) do
    results =
      Map.new(operation_ids, fn id ->
        {id, state.store_module.get(state.store, id)}
      end)

    {:reply, results, state}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    {count, new_store} = state.store_module.clear_queue(state.store, :canceled)
    Telemetry.emit_queue_cleared(state.config.name, count)
    Logger.info("[Ferry:#{state.config.name}] Cleared #{count} pending operations")
    {:reply, {:ok, count}, %{state | store: new_store, total_failed: state.total_failed + count}}
  end

  @impl true
  def handle_call({:pending, opts}, _from, state) do
    ops = state.store_module.list_pending(state.store, opts)
    {:reply, ops, state}
  end

  @impl true
  def handle_call({:completed, opts}, _from, state) do
    ops = state.store_module.list_completed(state.store, opts)
    {:reply, ops, state}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    queue_size = state.store_module.queue_size(state.store)

    state =
      if queue_size > 0 and state.flushing == nil do
        start_async_flush(state, :timer)
      else
        state
      end

    state =
      if state.status == :running do
        schedule_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  # Async flush result handler
  @impl true
  def handle_info({ref, {:flush_result, batch, ops, results, start_time, _trigger}}, state)
      when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    duration_ms = System.monotonic_time(:millisecond) - start_time
    now = DateTime.utc_now()
    name = state.config.name

    {succeeded, failed, state} = process_results(state, ops, results, batch.id, duration_ms, now)

    status_override = batch_status_override(results)

    Telemetry.emit_batch_completed(name, batch.id, batch.size, succeeded, failed, duration_ms,
      status_override: status_override
    )

    Logger.debug(
      "[Ferry:#{name}] Batch #{batch.id} completed: #{succeeded} ok, #{failed} failed in #{duration_ms}ms"
    )

    state = %{
      state
      | batches_executed: state.batches_executed + 1,
        total_batch_duration_ms: state.total_batch_duration_ms + duration_ms,
        last_flush_at: now,
        flushing: nil
    }

    # Reply to all waiting callers
    Enum.each(state.flush_callers, fn caller ->
      GenServer.reply(caller, :ok)
    end)

    {:noreply, %{state | flush_callers: []}}
  end

  # Async flush crash handler
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{flushing: %{ref: ref}} = state) do
    %{batch: batch, ops: ops, start_time: start_time} = state.flushing
    duration_ms = System.monotonic_time(:millisecond) - start_time
    name = state.config.name

    Logger.warning("[Ferry:#{name}] Batch #{batch.id} resolver crashed: #{inspect(reason)}")

    Telemetry.emit_batch_crashed(name, batch.id, batch.size, reason)

    stamped_ops = Enum.map(ops, &%{&1 | batch_id: batch.id})

    {:ok, new_store} =
      state.store_module.move_batch_to_dlq(state.store, stamped_ops, {:exit, reason})

    Enum.each(ops, fn op ->
      Telemetry.emit_dead_lettered(name, op.id, {:exit, reason})
    end)

    Telemetry.emit_batch_completed(name, batch.id, batch.size, 0, length(ops), duration_ms,
      status_override: :crashed
    )

    state = %{
      state
      | store: new_store,
        flushing: nil,
        batches_executed: state.batches_executed + 1,
        total_batch_duration_ms: state.total_batch_duration_ms + duration_ms,
        total_failed: state.total_failed + length(ops),
        last_flush_at: DateTime.utc_now()
    }

    Enum.each(state.flush_callers, fn caller ->
      GenServer.reply(caller, :ok)
    end)

    {:noreply, %{state | flush_callers: []}}
  end

  @impl true
  def handle_info(:purge_completed, state) do
    {purged, new_store} =
      state.store_module.purge_completed(
        state.store,
        state.config.completed_ttl,
        state.config.max_completed
      )

    if purged > 0 do
      Telemetry.emit_completed_purged(state.config.name, purged, :ttl)
    end

    schedule_purge()
    {:noreply, %{state | store: new_store}}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, {:heir_handback, _ferry_name}}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private

  defp start_async_flush(state, trigger) do
    name = state.config.name
    queue_size_before = state.store_module.queue_size(state.store)

    {ops, new_store} = state.store_module.pop_batch(state.store, state.config.batch_size)
    state = %{state | store: new_store}

    batch_id = Ferry.IdGenerator.generate(nil)

    batch = %Batch{
      id: batch_id,
      operations: ops,
      size: length(ops),
      ferry_name: name,
      flushed_at: DateTime.utc_now(),
      metadata: %{}
    }

    operation_ids = Enum.map(ops, & &1.id)

    Telemetry.emit_batch_started(
      name,
      batch_id,
      batch.size,
      queue_size_before,
      trigger,
      operation_ids
    )

    Logger.debug(
      "[Ferry:#{name}] Flushing batch #{batch_id}: #{batch.size} ops (trigger=#{trigger})"
    )

    start_time = System.monotonic_time(:millisecond)
    timeout = state.config.operation_timeout
    resolver = state.config.resolver

    # Execute resolver in a monitored Task — non-blocking
    task =
      Task.async(fn ->
        result = execute_resolver_fn(resolver, batch, timeout)
        {:flush_result, batch, ops, result, start_time, trigger}
      end)

    %{state | flushing: %{ref: task.ref, batch: batch, ops: ops, start_time: start_time}}
  end

  defp execute_resolver_fn(resolver, batch, timeout) do
    caller = self()
    mref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        try do
          result = resolver.(batch)
          send(caller, {mref, {:ok, result}})
        rescue
          e ->
            send(caller, {mref, {:error, {e, __STACKTRACE__}}})
        catch
          kind, reason ->
            send(caller, {mref, {:error, {kind, reason}}})
        end
      end)

    result =
      receive do
        {^mref, {:ok, results}} when is_list(results) ->
          results

        {^mref, {:ok, _bad_return}} ->
          :dlq_bad_return

        {^mref, {:error, reason}} ->
          {:dlq_error, reason}

        {:DOWN, ^monitor_ref, :process, ^pid, reason} when reason != :normal ->
          {:dlq_error, {:exit, reason}}
      after
        timeout ->
          Process.exit(pid, :kill)
          :dlq_timeout
      end

    # Flush DOWN message
    receive do
      {:DOWN, ^monitor_ref, :process, ^pid, _} -> :ok
    after
      0 -> :ok
    end

    # Flush late result
    receive do
      {^mref, _} -> :ok
    after
      0 -> :ok
    end

    result
  end

  defp process_results(state, ops, :dlq_timeout, batch_id, _duration_ms, _now) do
    name = state.config.name
    Logger.warning("[Ferry:#{name}] Batch #{batch_id} timed out, #{length(ops)} ops moved to DLQ")
    Telemetry.emit_batch_timeout(name, batch_id, length(ops), state.config.operation_timeout)

    stamped_ops = Enum.map(ops, &%{&1 | batch_id: batch_id})

    {:ok, new_store} =
      state.store_module.move_batch_to_dlq(state.store, stamped_ops, :timeout)

    Enum.each(ops, fn op ->
      Telemetry.emit_dead_lettered(name, op.id, :timeout)
    end)

    {0, length(ops), %{state | store: new_store, total_failed: state.total_failed + length(ops)}}
  end

  defp process_results(state, ops, :dlq_bad_return, batch_id, _duration_ms, _now) do
    name = state.config.name

    Logger.warning(
      "[Ferry:#{name}] Batch #{batch_id} bad return, #{length(ops)} ops moved to DLQ"
    )

    Telemetry.emit_batch_crashed(name, batch_id, length(ops), :bad_return)
    stamped_ops = Enum.map(ops, &%{&1 | batch_id: batch_id})

    {:ok, new_store} =
      state.store_module.move_batch_to_dlq(state.store, stamped_ops, :bad_return)

    Enum.each(ops, fn op ->
      Telemetry.emit_dead_lettered(name, op.id, :bad_return)
    end)

    {0, length(ops), %{state | store: new_store, total_failed: state.total_failed + length(ops)}}
  end

  defp process_results(state, ops, {:dlq_error, reason}, batch_id, _duration_ms, _now) do
    name = state.config.name

    Logger.warning(
      "[Ferry:#{name}] Batch #{batch_id} resolver error: #{inspect(reason)}, #{length(ops)} ops moved to DLQ"
    )

    Telemetry.emit_batch_crashed(name, batch_id, length(ops), reason)
    stamped_ops = Enum.map(ops, &%{&1 | batch_id: batch_id})

    {:ok, new_store} =
      state.store_module.move_batch_to_dlq(state.store, stamped_ops, {:exit, reason})

    Enum.each(ops, fn op ->
      Telemetry.emit_dead_lettered(name, op.id, {:exit, reason})
    end)

    {0, length(ops), %{state | store: new_store, total_failed: state.total_failed + length(ops)}}
  end

  defp process_results(state, ops, results, batch_id, duration_ms, now) when is_list(results) do
    result_map = Map.new(results, fn {id, status, value} -> {id, {status, value}} end)

    {succeeded, failed, store} =
      Enum.reduce(ops, {0, 0, state.store}, fn op, {s, f, st} ->
        case Map.get(result_map, op.id) do
          {:ok, result} ->
            {:ok, new_st} = state.store_module.mark_completed(st, op.id, result, now, batch_id)
            Telemetry.emit_completed(state.config.name, op.id, batch_id, duration_ms)
            {s + 1, f, new_st}

          {:error, error} ->
            {:ok, new_st} = state.store_module.mark_failed(st, op.id, error, now, batch_id)
            Telemetry.emit_failed(state.config.name, op.id, batch_id, error, duration_ms)
            Telemetry.emit_dead_lettered(state.config.name, op.id, error)
            {s, f + 1, new_st}

          nil ->
            {:ok, new_st} =
              state.store_module.mark_failed(st, op.id, :no_result_returned, now, batch_id)

            Telemetry.emit_failed(
              state.config.name,
              op.id,
              batch_id,
              :no_result_returned,
              duration_ms
            )

            Telemetry.emit_dead_lettered(state.config.name, op.id, :no_result_returned)
            {s, f + 1, new_st}
        end
      end)

    {succeeded, failed,
     %{
       state
       | store: store,
         total_processed: state.total_processed + succeeded,
         total_failed: state.total_failed + failed
     }}
  end

  defp build_operation(id, payload, order) do
    %Operation{
      id: id,
      payload: payload,
      order: order,
      status: :pending,
      pushed_at: DateTime.utc_now()
    }
  end

  defp generate_id(state, payload) do
    id = state.config.id_generator.(payload)
    {id, state}
  end

  defp build_stats(state) do
    avg =
      if state.batches_executed > 0 do
        state.total_batch_duration_ms / state.batches_executed
      else
        0.0
      end

    uptime_ms = DateTime.diff(DateTime.utc_now(), state.started_at, :millisecond)

    %Ferry.Stats{
      queue_size: state.store_module.queue_size(state.store),
      dlq_size: state.store_module.dlq_size(state.store),
      completed_size: state.store_module.completed_size(state.store),
      total_pushed: state.total_pushed,
      total_processed: state.total_processed,
      total_failed: state.total_failed,
      total_rejected: state.total_rejected,
      batches_executed: state.batches_executed,
      avg_batch_duration_ms: avg,
      last_flush_at: state.last_flush_at,
      uptime_ms: uptime_ms,
      memory_bytes: state.store_module.memory_bytes(state.store),
      status: state.status
    }
  end

  defp schedule_flush(state) do
    state = cancel_timer(state)
    ref = Process.send_after(self(), :flush_tick, state.config.flush_interval)
    %{state | timer_ref: ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer_ref: nil}
  end

  defp schedule_purge do
    Process.send_after(self(), :purge_completed, @purge_interval)
  end

  defp batch_status_override(:dlq_timeout), do: :timeout
  defp batch_status_override(:dlq_bad_return), do: :crashed
  defp batch_status_override({:dlq_error, _}), do: :crashed
  defp batch_status_override(_results), do: nil

  defp store_module(:memory), do: Ferry.Store.Memory
  defp store_module(:ets), do: Ferry.Store.Ets

  defp via(name), do: :"#{name}.Server"
end
