defmodule Parquer.EprofBench do
  def profile1(num_records \\ 2**16) do
    schema = :parquer_schema.root("root", [:parquer_schema.int64("f0", :required, %{})])
    writer0 = :parquer_writer.new(schema, %{enable_dictionary: true, data_page_header_version: 2})
    :eprof.profile(fn -> 1..num_records |> Enum.reduce(writer0, fn n, w -> {_, w} = :parquer_writer.write(w, %{"f0" => n}); w end) end)
    :eprof.analyze()
  end

  def profile2(num_records \\ 2**16) do
    schema = :parquer_schema.root("root", [:parquer_schema.group("root2", :repeated,  [:parquer_schema.int64("f0", :required, %{})])])
    writer0 = :parquer_writer.new(schema, %{enable_dictionary: true, data_page_header_version: 2})
    records = 1..num_records |> Enum.map(fn n -> %{"f0" => n} end)
    :eprof.profile(fn -> :parquer_writer.write_many(writer0, records) end)
    :eprof.analyze()
  end
end
