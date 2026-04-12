defmodule Ferry.IdGenerator do
  @moduledoc """
  Default ID generator for Ferry operations.

  Generates short, URL-safe, collision-resistant IDs with a `fry_` prefix.
  """

  @alphabet ~c"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  @alphabet_size length(@alphabet)

  @doc """
  Generates a prefixed ID like `"fry_KJ82mXa9pQ"`.

  The payload argument is accepted but ignored by the default generator.
  Custom generators can use it to derive deterministic IDs.
  """
  @spec generate(term()) :: String.t()
  def generate(_payload \\ nil) do
    "fry_" <> random_base62(10)
  end

  defp random_base62(length) do
    :crypto.strong_rand_bytes(length)
    |> :binary.bin_to_list()
    |> Enum.map(fn byte -> Enum.at(@alphabet, rem(byte, @alphabet_size)) end)
    |> List.to_string()
  end
end
