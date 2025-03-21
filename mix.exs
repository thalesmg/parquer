defmodule Paquer.MixProject do
  use Mix.Project

  def project() do
    [
      app: :parquer,
      language: :erlang,
      version: read_version(),
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:dev), do: ["dev"]
  defp elixirc_paths(_), do: []

  def application do
    [
      extra_applications: []
    ]
  end

  defp deps() do
    [
      {:thrift, github: "emqx/thrift.erl", tag: "0.1.3"},
      {:ezstd, github: "emqx/ezstd", tag: "v1.0.5-emqx1"},
      {:snappyer, "1.2.10"},
      {:redbug, github: "emqx/redbug", tag: "2.0.10", only: [:dev, :test]}
    ]
  end

  defp read_version() do
    try do
      {out, 0} = System.cmd("git", ["describe", "--tags"])

      out
      |> String.trim()
      |> Version.parse!()
      |> Map.put(:pre, [])
      |> to_string()
    catch
      _, _ -> "0.1.0"
    end
  end
end
