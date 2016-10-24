{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
module Zebra.Foreign.Bindings where

import Anemone.Foreign.Data (CError(..))

--
-- This module contains 1:1 bindings for all the zebra header files, in the
-- style of bindings-DSL, for "nice" wrappers, see the other Zebra.Foreign.*
-- modules.
--

#include <bindings.dsl.h>
#include "zebra_bindings.h"
#include "zebra_data.h"

#strict_import

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
#field attribute_ids , Ptr Int64
#field attribute_row_counts , Ptr Int64
#stoptype

#starttype struct zebra_block
#field attribute_count , Int64
#field entity_count , Int64
#field entities , Ptr <zebra_block_entity>
#field row_count , Int64
#field times , Ptr Int64
#field priorities , Ptr Int64
#field tombstones , Ptr Int64
#field tables , Ptr <zebra_table>
#stoptype
