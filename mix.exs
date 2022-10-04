defmodule Khepri.MixProject do
  use Mix.Project

  def project do
    # To avoid duplication, we query the app file to learn the application
    # name, description and version.
    {:ok, [app]} = :file.consult("src/khepri.app.src")
    {:application, app_name, props} = app

    description = to_string(Keyword.get(props, :description))
    version = to_string(Keyword.get(props, :vsn))

    [
      app: app_name,
      description: description,
      version: version,
      language: :erlang,
      deps: deps()
    ]
  end

  def application do
    {:ok, [app]} = :file.consult("src/khepri.app.src")
    {:application, _app_name, props} = app

    Keyword.take(props, [:applications, :env, :mod, :registered])
  end

  defp deps() do
    # To avoid duplication, we query rebar.config to get the list of
    # dependencies and their version pinning.
    {:ok, terms} = :file.consult("rebar.config")
    deps = Keyword.get(terms, :deps)

    # The conversion to the mix.exs expected structure is basic, but that
    # should do it for our needs.
    for {app_name, version} <- deps do
      case version do
        _ when is_list(version) ->
          {app_name, to_string(version)}

        {:git, url} ->
          {app_name, git: to_string(url)}

        {:git, url, {:ref, ref}} ->
          {app_name, git: to_string(url), ref: to_string(ref)}

        {:git, url, {:branch, branch}} ->
          {app_name, git: to_string(url), branch: to_string(branch)}

        {:git, url, {:tag, tag}} ->
          {app_name, git: to_string(url), tag: to_string(tag)}
      end
    end
  end
end
