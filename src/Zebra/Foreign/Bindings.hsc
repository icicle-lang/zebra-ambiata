{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
module Zebra.Foreign.Bindings where

import Anemone.Foreign.Data (CError(..))
import Anemone.Foreign.Mempool (Mempool(..))

--
-- This module contains 1:1 bindings for all the zebra header files, in the
-- style of bindings-DSL, for "nice" wrappers, see the other Zebra.Foreign.*
-- modules.
--

#include <bindings.dsl.h>
#include "zebra_bindings.h"
#include "zebra_data.h"
#include "zebra_hash.h"
#include "zebra_merge_many.h"

#strict_import

-- Hash used for plonking
#znum ZEBRA_HASH_SEED

-- CError / ForeignError enums
#znum ZEBRA_SUCCESS
#znum ZEBRA_INVALID_COLUMN_TYPE
#znum ZEBRA_INVALID_TABLE_TYPE
#znum ZEBRA_ATTRIBUTE_NOT_FOUND
#znum ZEBRA_NOT_ENOUGH_BYTES
#znum ZEBRA_NOT_ENOUGH_ROWS
#znum ZEBRA_MERGE_NO_ENTITIES
#znum ZEBRA_APPEND_DIFFERENT_COLUMN_TYPES
#znum ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT


-- Zebra.Table.Table
#integral_t enum zebra_table_tag
#znum ZEBRA_TABLE_BINARY
#znum ZEBRA_TABLE_ARRAY
#znum ZEBRA_TABLE_MAP

#starttype union zebra_table_variant
-- ZEBRA_TABLE_BINARY
#field _binary.bytes    , Ptr Word8
-- ZEBRA_TABLE_ARRAY
#field _array.values    , Ptr <zebra_column>
-- ZEBRA_TABLE_MAP
#field _map.keys        , Ptr <zebra_column>
#field _map.values      , Ptr <zebra_column>
#stoptype

#starttype struct zebra_table
#field row_count        , Int64
#field row_capacity     , Int64
#field tag              , <zebra_table_tag>
#field of               , <zebra_table_variant>
#stoptype


-- Vector (Variant, Column)
-- Vector (Field, Column)
#starttype struct zebra_named_columns
#field count            , Int64
#field columns          , Ptr <zebra_column>
#field name_lengths     , Ptr Int64
#field name_lengths_sum , Int64
#field name_bytes       , Ptr Word8
#stoptype


-- Zebra.Table.Column
#integral_t enum zebra_column_tag
#znum ZEBRA_COLUMN_UNIT
#znum ZEBRA_COLUMN_INT
#znum ZEBRA_COLUMN_DOUBLE
#znum ZEBRA_COLUMN_ENUM
#znum ZEBRA_COLUMN_STRUCT
#znum ZEBRA_COLUMN_NESTED
#znum ZEBRA_COLUMN_REVERSED

#starttype union zebra_column_variant
-- ZEBRA_COLUMN_UNIT (empty)
-- ZEBRA_COLUMN_INT
#field _int.values      , Ptr Int64
-- ZEBRA_COLUMN_DOUBLE
#field _double.values   , Ptr Double
-- ZEBRA_COLUMN_ENUM
#field _enum.tags       , Ptr Int64
#field _enum.columns    , <zebra_named_columns>
-- ZEBRA_COLUMN_STRUCT
#field _struct.columns  , <zebra_named_columns>
-- ZEBRA_COLUMN_NESTED
#field _nested.indices  , Ptr Int64
#field _nested.table    , <zebra_table>
-- ZEBRA_COLUMN_REVERSED
#field _reversed.column , Ptr <zebra_column>
#stoptype

#starttype struct zebra_column
#field tag              , <zebra_column_tag>
#field of               , <zebra_column_variant>
#stoptype


---------------------------
-- Attributes and entities
---------------------------
#starttype struct zebra_attribute
#field times , Ptr Int64
#field factset_ids , Ptr Int64
#field tombstones , Ptr Int64
#field table , <zebra_table>
#stoptype

#starttype struct zebra_entity
#field hash , Word32
#field id_length , Int64
#field id_bytes , Ptr Word8
#field attribute_count , Int64
#field attributes , Ptr <zebra_attribute>
#stoptype

#starttype struct zebra_block_entity
#field hash , Word32
#field id_length , Int64
#field id_bytes , Ptr Word8
#field attribute_count , Int64
#field attribute_ids , Ptr Int64
#field attribute_row_counts , Ptr Int64
#stoptype

#starttype struct zebra_block
#field entity_count , Int64
#field entities , Ptr <zebra_block_entity>
#field row_count , Int64
#field times , Ptr Int64
#field factset_ids , Ptr Int64
#field tombstones , Ptr Int64
#field table_count , Int64
#field tables , Ptr <zebra_table>
#stoptype


---------------------------
-- Struct for merging entities
---------------------------
#starttype struct zebra_merge_many
#field count , Int64
#field entities , Ptr <zebra_entity>
#stoptype

#ccall_unsafe zebra_append_block_entity , Mempool -> Ptr <zebra_entity> -> Ptr (Ptr <zebra_block>) -> IO CError
#ccall_unsafe zebra_entities_of_block , Mempool -> Ptr <zebra_block> -> Ptr Int64 -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall_unsafe zebra_merge_entity_pair , Mempool -> Ptr <zebra_entity> -> Ptr <zebra_entity> -> Ptr <zebra_entity> -> IO CError
#ccall_unsafe zebra_mm_init , Mempool -> Ptr (Ptr <zebra_merge_many>) -> IO CError
#ccall_unsafe zebra_mm_push , Mempool -> Ptr <zebra_merge_many> -> Int64 -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall_unsafe zebra_mm_pop , Mempool -> Ptr <zebra_merge_many> -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall_unsafe zebra_mm_clone , Mempool -> Ptr (Ptr <zebra_merge_many>) -> IO CError

#ccall_unsafe zebra_unpack_array , Ptr CChar -> Int64 -> Int64 -> Int64 -> Ptr Int64 -> IO CError
#ccall_unsafe zebra_pack_array , Ptr (Ptr Word8) -> Ptr Int64 -> Int64 -> IO CError

