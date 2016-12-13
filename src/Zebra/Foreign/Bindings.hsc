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
#include "zebra_merge_many.h"

#strict_import

#znum ZEBRA_SUCCESS
#znum ZEBRA_INVALID_COLUMN_TYPE
#znum ZEBRA_ATTRIBUTE_NOT_FOUND
#znum ZEBRA_NOT_ENOUGH_BYTES
#znum ZEBRA_NOT_ENOUGH_ROWS
#znum ZEBRA_MERGE_DIFFERENT_COLUMN_TYPES
#znum ZEBRA_MERGE_DIFFERENT_ENTITIES
#znum ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT

#integral_t enum zebra_type
#znum ZEBRA_BYTE
#znum ZEBRA_INT
#znum ZEBRA_DOUBLE
#znum ZEBRA_ARRAY

#starttype struct zebra_table
#field row_count , Int64
#field row_capacity , Int64
#field column_count , Int64
#field columns , Ptr <zebra_column>
#stoptype

#starttype union zebra_data
#field b , Ptr Word8
#field i , Ptr Int64
#field d , Ptr Double
#field a.n , Ptr Int64
#field a.table , <zebra_table>
#stoptype

#starttype struct zebra_column
#field type , <zebra_type>
#field data , <zebra_data>
#stoptype

#starttype struct zebra_attribute
#field times , Ptr Int64
#field priorities , Ptr Int64
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
#field priorities , Ptr Int64
#field tombstones , Ptr Int64
#field table_count , Int64
#field tables , Ptr <zebra_table>
#stoptype

#starttype struct zebra_merge_many
#field count , Int64
#field entities , Ptr <zebra_entity>
#stoptype

#ccall zebra_append_block_entity , Mempool -> Ptr <zebra_entity> -> Ptr (Ptr <zebra_block>) -> IO CError
#ccall zebra_entities_of_block , Mempool -> Ptr <zebra_block> -> Ptr Int64 -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall zebra_merge_entity , Mempool -> Ptr <zebra_entity> -> Ptr <zebra_entity> -> Ptr <zebra_entity> -> IO CError
#ccall zebra_mm_init , Mempool -> Ptr (Ptr <zebra_merge_many>) -> IO CError
#ccall zebra_mm_push , Mempool -> Ptr <zebra_merge_many> -> Int64 -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall zebra_mm_pop , Ptr <zebra_merge_many> -> Ptr (Ptr <zebra_entity>) -> IO CError
#ccall zebra_mm_clone , Mempool -> Ptr (Ptr <zebra_merge_many>) -> IO CError

