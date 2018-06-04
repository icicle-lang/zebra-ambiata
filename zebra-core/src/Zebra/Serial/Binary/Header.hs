{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Serial.Binary.Header (
    bHeader
  , bVersion

  , getHeader
  , getVersion

  -- * Internal
  , bHeaderV3
  , getHeaderV3
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text as Text

import           P

import           Zebra.Serial.Binary.Array
import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Json.Schema
import qualified Zebra.Table.Schema as Schema


-- | Encode a zebra header.
--
--   header {
--     "||ZEBRA||vvvvv||" : 16 x u8
--     header             : header_v2 | header_v3
--   }
--
bHeader :: Header -> Builder
bHeader = \case
  HeaderV3 x ->
    bVersion BinaryV3 <>
    bHeaderV3 x
{-# INLINABLE bHeader #-}

getHeader :: Get Header
getHeader = do
  version <- getVersion
  case version of
    BinaryV3 ->
      HeaderV3 <$> getHeaderV3
{-# INLINABLE getHeader #-}

-- | Encode a zebra v3 header from a dictionary.
--
-- @
--   header_v3 {
--     schema : sized_byte_array
--   }
-- @
bHeaderV3 :: Schema.Table -> Builder
bHeaderV3 schema =
  bSizedByteArray (encodeSchema SchemaV1 schema)
{-# INLINABLE bHeaderV3 #-}

getHeaderV3 :: Get Schema.Table
getHeaderV3 =
  parseSchema SchemaV1 =<< getSizedByteArray
{-# INLINABLE getHeaderV3 #-}

parseSchema :: SchemaVersion -> ByteString -> Get Schema.Table
parseSchema version =
  either (fail . Text.unpack . renderJsonSchemaDecodeError) pure .
  decodeSchema version
{-# INLINABLE parseSchema #-}

-- | The zebra 8-byte magic number, including version.
--
-- @
-- ||ZEBRA||vvvvv||
-- @
bVersion :: BinaryVersion -> Builder
bVersion = \case
   BinaryV3 ->
    Builder.byteString MagicV3
{-# INLINABLE bVersion #-}

getVersion :: Get BinaryVersion
getVersion = do
  bs <- Get.getByteString $ ByteString.length MagicV2
  case bs of
    MagicV0 ->
      fail $ "This version of zebra cannot read v0 zebra files."
    MagicV1 ->
      fail $ "This version of zebra cannot read v1 zebra files."
    MagicV2 ->
      fail $ "This version of zebra cannot read v2 zebra files."
    MagicV3 ->
      pure BinaryV3
    _ ->
      fail $ "Invalid/unknown file signature: " <> show bs
{-# INLINABLE getVersion #-}

#if __GLASGOW_HASKELL__ >= 800
pattern MagicV0 :: ByteString
#endif
pattern MagicV0 =
  "||ZEBRA||00000||"

#if __GLASGOW_HASKELL__ >= 800
pattern MagicV1 :: ByteString
#endif
pattern MagicV1 =
  "||ZEBRA||00001||"

#if __GLASGOW_HASKELL__ >= 800
pattern MagicV2 :: ByteString
#endif
pattern MagicV2 =
  "||ZEBRA||00002||"

#if __GLASGOW_HASKELL__ >= 800
pattern MagicV3 :: ByteString
#endif
pattern MagicV3 =
  "||ZEBRA||00003||"
