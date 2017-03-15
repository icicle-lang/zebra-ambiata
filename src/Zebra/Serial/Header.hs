{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Serial.Header (
    bHeader
  , bVersion

  , getHeader
  , getVersion
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString as ByteString
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Data.Core
import           Zebra.Schema (TableSchema)
import qualified Zebra.Schema as Schema
import           Zebra.Serial.Array

#if __GLASGOW_HASKELL__ >= 800
import           Data.ByteString (ByteString)
#endif


-- | Encode the zebra header from a dictionary.
--
-- @
--   header {
--     "||ZEBRA||vvvvv||" : 16 x u8
--     attr_count         : u32
--     attr_name_length   : int_array schema_count
--     attr_name_string   : byte_array
--     attr_schema_length : int_array schema_count
--     attr_schema_string : byte_array
--   }
-- @
bHeader :: ZebraVersion -> Map AttributeName TableSchema -> Builder
bHeader version features =
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
      fmap Schema.encode .
      Boxed.fromList $
      Map.elems features
  in
    bVersion version <>
    n_attrs <>
    names <>
    case version of
      ZebraV2 ->
        schema

getHeader :: Get (Map AttributeName TableSchema)
getHeader = do
  version <- getVersion
  n <- fromIntegral <$> Get.getWord32le
  ns <- fmap (AttributeName . Text.decodeUtf8) <$> getStrings n

  let
    parse =
      case version of
        ZebraV2 ->
          either (fail . Text.unpack . Schema.renderSchemaDecodeError) pure . Schema.decode

  ss <- traverse parse =<< getStrings n
  pure .
    Map.fromList . toList $
    Boxed.zip ns ss

-- | The zebra 8-byte magic number, including version.
--
-- @
-- ||ZEBRA||vvvvv||
-- @
bVersion :: ZebraVersion -> Builder
bVersion = \case
  ZebraV2 ->
    Builder.byteString MagicV2

getVersion :: Get ZebraVersion
getVersion = do
  bs <- Get.getByteString $ ByteString.length MagicV2
  case bs of
    MagicV2 ->
      pure ZebraV2
    _ ->
      fail $ "Invalid magic number: " <> show bs

#if __GLASGOW_HASKELL__ >= 800
pattern MagicV2 :: ByteString
#endif
pattern MagicV2 =
  "||ZEBRA||00002||"
