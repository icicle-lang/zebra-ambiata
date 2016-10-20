{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Header (
    bHeader
  , bMagic

  , getHeader
  , getMagic
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
import           Zebra.Data.Schema
import           Zebra.Serial.Array


-- | Encode the zebra header from a dictionary.
--
-- @
--   header {
--     "||ZEBRA||00000||" : 16 x u8
--     attr_count         : u32
--     attr_name_length   : int_array schema_count
--     attr_name_string   : byte_array
--     attr_schema_length : int_array schema_count
--     attr_schema_string : byte_array
--   }
-- @
bHeader :: Map AttributeName Schema -> Builder
bHeader features =
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
      fmap (T.encodeUtf8 . renderSchema) .
      Boxed.fromList $
      Map.elems features
  in
    bMagic <>
    n_attrs <>
    names <>
    schema

getHeader :: Get (Map AttributeName Schema)
getHeader = do
  () <- getMagic
  n <- fromIntegral <$> Get.getWord32le
  ns <- fmap (AttributeName . T.decodeUtf8) <$> getStrings n
  ss <- traverse (fromBytes "schema" parseSchema) =<< getStrings n
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
--   ||ZEBRA||00000||
-- @
magic :: ByteString
magic =
  "||ZEBRA||00000||"

bMagic :: Builder
bMagic =
  Build.byteString magic

getMagic :: Get ()
getMagic = do
  bs <- Get.getByteString (B.length magic)
  when (bs /= magic) $
    fail $ "Invalid magic number: " <> show bs
  pure ()
