defmodule Parquer.Records do
  require Record

  Module.register_attribute(__MODULE__, :rec, accumulate: true)

  Record.extract_all(from: "../include/parquer_parquet_types.hrl")
  |> Enum.map(fn {name, fields} ->
    Module.put_attribute(__MODULE__, :rec, name)
    Record.defrecord(name, fields)
  end)

  @names Module.get_attribute(__MODULE__, :rec, [])
  def names(), do: @names

  defmacro __before_compile__(env) do
    names = __MODULE__.names()

    kw_clauses =
      Enum.map(names, fn n ->
        quote do
          def to_kw(rec) when Record.is_record(rec, unquote(n)) do
            rec
            |> unquote(__MODULE__).unquote(n)()
            |> Enum.map(fn {k, v} ->
              {k, to_kw(v)}
            end)
          end
        end
      end)

    map_clauses =
      Enum.map(names, fn n ->
        quote do
          def to_map(rec) when Record.is_record(rec, unquote(n)) do
            rec
            |> unquote(__MODULE__).unquote(n)()
            |> Map.new(fn {k, v} ->
              {k, to_map(v)}
            end)
            |> Map.put(:"$type", unquote(n))
          end
        end
      end)

    quote do
      unquote_splicing(kw_clauses)

      def to_kw(xs) when is_list(xs) do
        Enum.map(xs, &to_kw/1)
      end

      def to_kw(x) do
        x
      end

      unquote_splicing(map_clauses)

      def to_map(xs) when is_list(xs) do
        Enum.map(xs, &to_map/1)
      end

      def to_map(x) do
        x
      end
    end
  end
end
