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
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as Boxed

import           P

import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Schema (TableSchema)
import qualified Zebra.Schema as Schema
import           Zebra.Serial.Array


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
      Build.word32LE . fromIntegral $
      Map.size features

    names =
      bStrings .
      fmap (T.encodeUtf8 . unAttributeName) .
      Boxed.fromList $
      Map.keys features

    schema =
      bStrings .
      fmap Schema.encode .
      Boxed.fromList $
      Map.elems features

    encoding =
      bStrings .
      fmap (T.encodeUtf8 . renderEncoding . encodingOfSchema) .
      Boxed.fromList $
      Map.elems features
  in
    bVersion version <>
    n_attrs <>
    names <>
    case version of
      ZebraV1 ->
        encoding
      ZebraV2 ->
        schema

getHeader :: Get (Map AttributeName TableSchema)
getHeader = do
  version <- getVersion
  n <- fromIntegral <$> Get.getWord32le
  ns <- fmap (AttributeName . T.decodeUtf8) <$> getStrings n

  let
    parse =
      case version of
        ZebraV1 ->
          fmap recoverSchemaOfEncoding . fromBytes "encoding" parseEncoding
        ZebraV2 ->
          either (fail . T.unpack . Schema.renderSchemaDecodeError) pure . Schema.decode

  ss <- traverse parse =<< getStrings n
  pure .
    Map.fromList . toList $
    Boxed.zip ns ss

fromBytes :: Monad m => Text -> (Text -> Maybe a) -> ByteString -> m a
fromBytes name parse bs =
  case parse $ T.decodeUtf8 bs of
    Nothing ->
      fail $ "Failed to parse " <> T.unpack name <> ": " <> show bs
    Just s ->
      pure s


-- | The zebra 8-byte magic number, including version.
--
-- @
-- ||ZEBRA||vvvvv||
-- @
bVersion :: ZebraVersion -> Builder
bVersion = \case
  ZebraV1 ->
    Build.byteString MagicV1
  ZebraV2 ->
    Build.byteString MagicV2

getVersion :: Get ZebraVersion
getVersion = do
  bs <- Get.getByteString $ B.length MagicV1
  case bs of
    MagicV1 ->
      pure ZebraV1
    MagicV2 ->
      pure ZebraV2
    _ ->
      fail $ "Invalid magic number: " <> show bs

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
