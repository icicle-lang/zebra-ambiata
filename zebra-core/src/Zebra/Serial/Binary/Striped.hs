{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Striped (
    bTable
  , bColumn
  , getTable
  , getColumn
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString (ByteString)
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import           Data.Coerce (coerce)
import qualified Data.Text as Text

import           P

import qualified X.Data.Vector.Cons as Cons
import qualified X.Data.Vector.Storable as Storable

import           Zebra.Serial.Binary.Array
import           Zebra.Serial.Binary.Data
import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


-- | Encode a zebra table as bytes.
--
bTable :: BinaryVersion -> Striped.Table -> Either BinaryEncodeError Builder
bTable version = \case
  Striped.Binary encoding bs -> do
    () <- first BinaryEncodeUtf8 $ Encoding.validateBinary encoding bs
    case version of
      BinaryV2 ->
        pure $ bSizedByteArray bs
      BinaryV3 ->
        pure $ bByteArray bs

  Striped.Array c ->
    bColumn version c

  Striped.Map k v ->
    (<>)
      <$> bColumn version k
      <*> bColumn version v

-- | Encode a zebra column as bytes.
--
bColumn :: BinaryVersion -> Striped.Column -> Either BinaryEncodeError Builder
bColumn version = \case
  Striped.Unit _ ->
    pure $ mempty

  Striped.Int xs ->
    pure $ bIntArray xs

  Striped.Double xs ->
    pure $ bDoubleArray xs

  Striped.Enum tags vs0 -> do
    vs <- traverse (bColumn version . variantData) vs0
    pure $
      bTagArray tags <>
      mconcat (Cons.toList vs)

  Striped.Struct fs ->
    mconcat . Cons.toList <$> traverse (bColumn version . fieldData) fs

  Striped.Nested ns x ->
    ((bIntArray ns <> Build.word32LE (fromIntegral $ Striped.length x)) <>)
      <$> bTable version x

  Striped.Reversed x ->
    bColumn version x

-- | Decode a zebra table using a row count and a schema.
--
getTable :: BinaryVersion -> Int -> Schema.Table -> Get Striped.Table
getTable version n = \case
  Schema.Binary encoding -> do
    bs <-
      validateBinary encoding =<<
      case version of
        BinaryV2 -> do
          getSizedByteArray

        BinaryV3 ->
          getByteArray n

    pure $ Striped.Binary encoding bs

  Schema.Array e ->
    Striped.Array
      <$> getColumn version n e

  Schema.Map k v ->
    Striped.Map
      <$> getColumn version n k
      <*> getColumn version n v

validateBinary :: Maybe Encoding.Binary -> ByteString -> Get ByteString
validateBinary encoding bs =
  case Encoding.validateBinary encoding bs of
    Left err ->
      fail . Text.unpack $
        renderBinaryDecodeError (BinaryDecodeUtf8 err)
    Right () ->
      pure bs

-- | Decode a zebra column using a row count and a schema.
--
getColumn :: BinaryVersion -> Int -> Schema.Column -> Get Striped.Column
getColumn version n = \case
  Schema.Unit ->
    pure $ Striped.Unit n

  Schema.Int ->
    Striped.Int
      <$> getIntArray n

  Schema.Double ->
    Striped.Double
      <$> getDoubleArray n

  Schema.Enum vs ->
    Striped.Enum
      <$> getTagArray n
      <*> Cons.mapM (traverse $ getColumn version n) vs

  Schema.Struct fs ->
    Striped.Struct
      <$> Cons.mapM (traverse $ getColumn version n) fs

  Schema.Nested t ->
    Striped.Nested
      <$> getIntArray n
      <*> (flip (getTable version) t . fromIntegral =<< Get.getWord32le)

  Schema.Reversed c ->
    Striped.Reversed
      <$> getColumn version n c

bTagArray :: Storable.Vector Tag -> Builder
bTagArray =
  bIntArray . coerce

getTagArray :: Int -> Get (Storable.Vector Tag)
getTagArray n =
  coerce <$> getIntArray n

bDoubleArray :: Storable.Vector Double -> Builder
bDoubleArray =
  bIntArray . coerce

getDoubleArray :: Int -> Get (Storable.Vector Double)
getDoubleArray n =
  coerce <$> getIntArray n
