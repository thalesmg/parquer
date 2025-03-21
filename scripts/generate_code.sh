#!/usr/bin/env bash
set -xeou pipefail

rm -rf gen-erl
thrift -r --gen erl:maps,legacynames,app_prefix=parquer_ -I proto priv/parquet.thrift
cp gen-erl/*.hrl include/
cp gen-erl/*.erl src/thrift/
rm -rf gen-erl
