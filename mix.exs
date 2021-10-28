defmodule Khepri.MixProject do
  use Mix.Project

  def project do
    [
      app: :khepri,
      description: "Tree-like replicated on-disk database library",
      version: "0.1.0",
      language: :erlang,
      deps: deps()
    ]
  end

  defp deps() do
    [
      {:ra,
        git: "https://github.com/rabbitmq/ra.git",
        ref: "1411b269e167e48aeb882911956291318806b3c3"}
    ]
  end
end
