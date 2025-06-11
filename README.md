# `parquer` - write [Parquet](https://parquet.apache.org/) files with pure Erlang

[![test](https://github.com/emqx/parquer/actions/workflows/test.yaml/badge.svg)](https://github.com/emqx/parquer/actions/workflows/test.yaml)

## Installation

Rebar3:

```erlang
{parquer, {git, "https://github.com/emqx/parquer.git", {tag, "0.1.0"}}}
```

Mix:

```elixir
{:parquer, github: "emqx/parquer", tag: "0.1.0", manager: :rebar3}}
```

## Quick start

```erlang
%% Define a schema
Schema =
  parquer_schema:root(
    <<"root">>,
    [ parquer_schema:string(<<"f0">>, optional)
    , parquer_schema:bool(<<"f1">>, required)
    ]).
%% Create a writer
Writer0 = parquer_writer:new(Schema, _WriterOpts = #{}).
%% Append records
{IOData1, Writer1} =
  parquer_writer:write_many(Writer0, [
    #{<<"f0">> => <<"hello">>, <<"f1">> => true},
    #{<<"f0">> => undefined, <<"f1">> => false}
    #{<<"f0">> => <<"world!">>, <<"f1">> => false}
  ]).
%% Finish writing
{IOData2, _WriteMetadata} = parquer_writer:close(Writer1).
%% Save data to a file
file:write_file("/tmp/data.parquet", [IOData1, IOData2]).
```

Testing the output:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r dev/dev_requirements.txt
python -c "from fastparquet import ParquetFile; pf = ParquetFile('/tmp/data.parquet'); print(pf.info); pf.head(10)"
deactivate
```

## Development

Prerequisites:

- [Erlang/OTP 27+](https://www.erlang.org/downloads)
- [Rebar3](https://rebar3.org/)
- [Elixir 1.17+](https://elixir-lang.org/install.html) (optional, for Mix support)
- [Apache Thrift](https://thrift.apache.org/)

```
# scripts/generate_code.sh ## only needed if `priv/parquet.thrift` is changed
scripts/format.sh fix
rebar3 compile
```
