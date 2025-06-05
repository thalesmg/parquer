#!/usr/bin/env bash
set -xeou pipefail

rm -rf gen-erl
thrift -r --gen erl:maps,legacynames,app_prefix=parquer_ -I proto priv/parquet.thrift
cp gen-erl/*.hrl include/
cp gen-erl/*.erl src/thrift/
rm -rf gen-erl

## patches to silence dialyzer
echo '-dialyzer(no_improper_lists).' >> src/thrift/parquer_parquet_types.erl
echo '-dialyzer({nowarn_function, struct_info_ext/1}).' >> src/thrift/parquer_parquet_types.erl
