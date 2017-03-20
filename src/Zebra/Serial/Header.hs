{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Serial.Header (
    Header(..)

  , headerOfAttributes
  , attributesOfHeader

  , bHeader
  , bVersion

  , getHeader
  , getVersion

  -- * Internal
  , bHeaderV3
  , getHeaderV3

  , bHeaderV2
  , getHeaderV2
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Schema (TableSchema, ColumnSchema)
import qualified Zebra.Schema as Schema
import           Zebra.Serial.Array


data Header =
    HeaderV2 !(Map AttributeName ColumnSchema)
  | HeaderV3 !TableSchema
    deriving (Eq, Ord, Show)

headerOfAttributes :: ZebraVersion -> Map AttributeName ColumnSchema -> Header
headerOfAttributes version attributes =
  case version of
    ZebraV2 ->
      HeaderV2 attributes
    ZebraV3 ->
      HeaderV3 (tableSchemaOfAttributes attributes)

attributesOfHeader :: Header -> Either BlockTableError (Map AttributeName ColumnSchema)
attributesOfHeader = \case
  HeaderV2 attributes ->
    pure attributes
  HeaderV3 table ->
    attributesOfTableSchema table

-- | Encode a zebra header.
--
--   header {
--     "||ZEBRA||vvvvv||" : 16 x u8
--     header             : header_v2 | header_v3
--   }
--
bHeader :: Header -> Builder
bHeader = \case
  HeaderV2 x ->
    bVersion ZebraV2 <>
    bHeaderV2 x
  HeaderV3 x ->
    bVersion ZebraV3 <>
    bHeaderV3 x

getHeader :: Get Header
getHeader = do
  version <- getVersion
  case version of
    ZebraV2 ->
      HeaderV2 <$> getHeaderV2
    ZebraV3 ->
      HeaderV3 <$> getHeaderV3

-- | Encode a zebra v3 header from a dictionary.
--
-- @
--   header_v3 {
--     schema : sized_byte_array
--   }
-- @
bHeaderV3 :: TableSchema -> Builder
bHeaderV3 schema =
  bSizedByteArray (Schema.encode schema)

getHeaderV3 :: Get TableSchema
getHeaderV3 =
  parseSchema =<< getSizedByteArray

-- | Encode a zebra v2 header from a dictionary.
--
-- @
--   header_v2 {
--     attr_count         : u32
--     attr_name_length   : int_array schema_count
--     attr_name_string   : sized_byte_array
--     attr_schema_length : int_array schema_count
--     attr_schema_string : sized_byte_array
--   }
-- @
bHeaderV2 :: Map AttributeName ColumnSchema -> Builder
bHeaderV2 features =
  let
    n_attrs =
      Builder.word32LE . fromIntegral $
      Map.size features

    names =
      bStrings .
      fmap (Text.encodeUtf8 . unAttributeName) .
      Boxed.fromList $
      Map.keys features

    schema =
      bStrings .
      fmap (Schema.encode . Schema.Array) .
      Boxed.fromList $
      Map.elems features
  in
    n_attrs <>
    names <>
    schema

getHeaderV2 :: Get (Map AttributeName ColumnSchema)
getHeaderV2 = do
  n <- fromIntegral <$> Get.getWord32le
  ns <- fmap (AttributeName . Text.decodeUtf8) <$> getStrings n
  ts <- traverse parseSchema =<< getStrings n

  let
    cs =
      either (fail . Text.unpack . Schema.renderSchemaError) id $
      traverse Schema.takeArray ts

  pure .
    Map.fromList . toList $
    Boxed.zip ns cs

parseSchema :: ByteString -> Get TableSchema
parseSchema =
  either (fail . Text.unpack . Schema.renderSchemaDecodeError) pure . Schema.decode

-- | The zebra 8-byte magic number, including version.
--
-- @
-- ||ZEBRA||vvvvv||
-- @
bVersion :: ZebraVersion -> Builder
bVersion = \case
  ZebraV2 ->
    Builder.byteString MagicV2
  ZebraV3 ->
    Builder.byteString MagicV3

getVersion :: Get ZebraVersion
getVersion = do
  bs <- Get.getByteString $ ByteString.length MagicV2
  case bs of
    MagicV0 ->
      fail $ "This version of zebra cannot read v0 zebra files."
    MagicV1 ->
      fail $ "This version of zebra cannot read v1 zebra files."
    MagicV2 ->
      pure ZebraV2
    MagicV3 ->
      pure ZebraV3
    _ ->
      fail $ "Invalid/unknown file signature: " <> show bs

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
