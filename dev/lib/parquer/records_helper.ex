defmodule Parquer.RecordsHelper do
  require Record
  require Parquer.Records
  @before_compile Parquer.Records

  # def to_map(rec) do
  #   [type | _] = rec |> Tuple.to_list()

  #   rec
  #   |> to_kw()
  #   |> Map.new()
  #   |> Map.put(:"$type", type)
  # end
end
