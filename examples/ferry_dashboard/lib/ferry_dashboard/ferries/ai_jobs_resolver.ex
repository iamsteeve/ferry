defmodule FerryDashboard.Ferries.AiJobsResolver do
  @moduledoc """
  Simulates bulk AI job processing.
  Slow processing (500-2000ms), higher failure rate (~12%).
  """

  def resolve(%Ferry.Batch{operations: ops}) do
    Enum.map(ops, fn op ->
      Process.sleep(Enum.random(500..2000))

      if :rand.uniform(100) <= 12 do
        errors = Enum.random([:model_overloaded, :context_too_long, :rate_limited, :gpu_oom])
        {op.id, :error, errors}
      else
        model = op.payload[:model] || Enum.random(["claude-4", "gpt-5", "gemini-3"])
        tokens = :rand.uniform(4000) + 500

        {op.id, :ok, %{model: model, tokens_used: tokens, latency_ms: :rand.uniform(1500) + 200}}
      end
    end)
  end
end
