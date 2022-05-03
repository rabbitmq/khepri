defmodule Khepri do
  @moduledoc """
  Elixir bindings for Khepri
  """

  @doc """
  Creates a native path or pattern from a unix string

  ## Examples

      iex> ~p"/stock/wood/:oak"
      ["stock", "wood", :oak]
  """
  def sigil_p(path, _opts) do
    :khepri_path.from_string(path)
  end

  @doc """
  Creates a native path or pattern at compile-time

  ## Examples

      iex> ~P"/stock/wood/:oak"
      ["stock", "wood", :oak]
  """
  defmacro sigil_P({:<<>>, _meta, [path]}, _opts) do
    path
    |> :khepri_path.from_string()
    |> Macro.escape()
  end
end
