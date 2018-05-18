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

#strict_import

-- CError / ForeignError enums
#znum ZEBRA_SUCCESS
#znum ZEBRA_UNPACK_BUFFER_TOO_SMALL
#znum ZEBRA_UNPACK_BUFFER_TOO_LARGE

#ccall_unsafe zebra_unpack_array , Ptr CChar -> Int64 -> Int64 -> Int64 -> Ptr Int64 -> IO CError
#ccall_unsafe zebra_pack_array , Ptr (Ptr Word8) -> Ptr Int64 -> Int64 -> IO CError
