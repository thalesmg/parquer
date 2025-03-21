%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-ifndef(PARQUER_THRIFT_HRL).
-define(PARQUER_THRIFT_HRL, true).

-define(ENCODING_PLAIN, plain).
-define(ENCODING_PLAIN_DICT, plain_dict).
-define(ENCODING_RLE, rle).
-define(ENCODING_RLE_DICT, rle_dict).

-define(PAGE_TYPE_DATA_PAGE_V1, data_page_v1).
-define(PAGE_TYPE_DATA_PAGE_V2, data_page_v2).
-define(PAGE_TYPE_DICT_PAGE, dict_page).

-define(codec, codec).
-define(column_chunks, column_chunks).
-define(compressed_page_size, compressed_page_size).
-define(data_page_header_v1, data_page_header_v1).
-define(data_page_header_v2, data_page_header_v2).
-define(data_page_offset, data_page_offset).
-define(definition_level_encoding, definition_level_encoding).
-define(definition_levels_byte_length, definition_levels_byte_length).
-define(dict_page_header, dict_page_header).
-define(dict_page_offset, dict_page_offset).
-define(encoding, encoding).
-define(encodings, encodings).
-define(is_sorted, is_sorted).
-define(metadata, metadata).
-define(page_type, page_type).
-define(path_in_schema, path_in_schema).
-define(repetition_level_encoding, repetition_level_encoding).
-define(repetition_levels_byte_length, repetition_levels_byte_length).
-define(row_groups, row_groups).
-define(schema, schema).
-define(total_compressed_size, total_compressed_size).
-define(total_uncompressed_size, total_uncompressed_size).
-define(type, type).
-define(uncompressed_page_size, uncompressed_page_size).
-define(version, version).

-endif.
