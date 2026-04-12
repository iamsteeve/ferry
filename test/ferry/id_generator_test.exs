defmodule Ferry.IdGeneratorTest do
  use ExUnit.Case, async: true

  alias Ferry.IdGenerator

  test "generates IDs with fry_ prefix" do
    id = IdGenerator.generate(nil)
    assert String.starts_with?(id, "fry_")
  end

  test "generates 10-character suffix" do
    "fry_" <> suffix = IdGenerator.generate(nil)
    assert String.length(suffix) == 10
  end

  test "generates unique IDs" do
    ids = for _ <- 1..1000, do: IdGenerator.generate(nil)
    assert length(Enum.uniq(ids)) == 1000
  end

  test "generates URL-safe characters (base62)" do
    for _ <- 1..100 do
      "fry_" <> suffix = IdGenerator.generate(nil)
      assert suffix =~ ~r/^[0-9A-Za-z]+$/
    end
  end

  test "accepts and ignores payload argument" do
    id = IdGenerator.generate(%{complex: :payload})
    assert String.starts_with?(id, "fry_")
  end
end
