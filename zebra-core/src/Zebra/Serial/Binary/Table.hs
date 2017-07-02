{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Table (
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

import qualified Neutron.Vector.Cons as Cons
import qualified Neutron.Vector.Storable as Storable

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
  Striped.Binary _def encoding bs -> do
    () <- first BinaryEncodeUtf8 $ Encoding.validateBinary encoding bs
    case version of
      BinaryV2 ->
        return $ bSizedByteArray bs
      BinaryV3 ->
        return $ bByteArray bs

  Striped.Array _def x ->
    bColumn version x

  Striped.Map _def k v ->
    (<>)
      <$> bColumn version k
      <*> bColumn version v
{-# INLINABLE bTable #-}

-- | Encode a zebra column as bytes.
--
bColumn :: BinaryVersion -> Striped.Column -> Either BinaryEncodeError Builder
bColumn version = \case
  Striped.Unit _ ->
    return $ mempty

  Striped.Int _def _encoding xs ->
    return $ bIntArray xs

  Striped.Double _def xs ->
    return $ bDoubleArray xs

  Striped.Enum _def tags vs0 -> do
    vs <- traverse (bColumn version . variantData) vs0
    return $
      bTagArray tags <>
      mconcat (Cons.toList vs)

  Striped.Struct _def fs ->
    mconcat . Cons.toList <$> traverse (bColumn version . fieldData) fs

  Striped.Nested ns x ->
    ((bIntArray ns <> Build.word32LE (fromIntegral $ Striped.length x)) <>)
      <$> bTable version x

  Striped.Reversed x ->
    bColumn version x
{-# INLINABLE bColumn #-}

-- | Decode a zebra table using a row count and a schema.
--
getTable :: BinaryVersion -> Int -> Schema.Table -> Get Striped.Table
getTable version n = \case
  Schema.Binary def encoding -> do
    bs <-
      validateBinary encoding =<<
      case version of
        BinaryV2 -> do
          getSizedByteArray

        BinaryV3 ->
          getByteArray n

    return $ Striped.Binary def encoding bs

  Schema.Array def x ->
    Striped.Array def
      <$> getColumn version n x

  Schema.Map def k v ->
    Striped.Map def
      <$> getColumn version n k
      <*> getColumn version n v
{-# INLINABLE getTable #-}

validateBinary :: Encoding.Binary -> ByteString -> Get ByteString
validateBinary encoding bs =
  case Encoding.validateBinary encoding bs of
    Left err ->
      fail . Text.unpack $
        renderBinaryDecodeError (BinaryDecodeUtf8 err)
    Right () ->
      return bs
{-# INLINABLE validateBinary #-}

-- | Decode a zebra column using a row count and a schema.
--
getColumn :: BinaryVersion -> Int -> Schema.Column -> Get Striped.Column
getColumn version n = \case
  Schema.Unit ->
    return $ Striped.Unit n

  Schema.Int def encoding ->
    Striped.Int def encoding
      <$> getIntArray n

  Schema.Double def ->
    Striped.Double def
      <$> getDoubleArray n

  Schema.Enum def vs ->
    Striped.Enum def
      <$> getTagArray n
      <*> Cons.mapM (traverse $ getColumn version n) vs

  Schema.Struct def fs ->
    Striped.Struct def
      <$> Cons.mapM (traverse $ getColumn version n) fs

  Schema.Nested t ->
    Striped.Nested
      <$> getIntArray n
      <*> (flip (getTable version) t . fromIntegral =<< Get.getWord32le)

  Schema.Reversed c ->
    Striped.Reversed
      <$> getColumn version n c
{-# INLINABLE getColumn #-}

bTagArray :: Storable.Vector Tag -> Builder
bTagArray =
  bIntArray . coerce
{-# INLINABLE bTagArray #-}

getTagArray :: Int -> Get (Storable.Vector Tag)
getTagArray n =
  coerce <$> getIntArray n
{-# INLINABLE getTagArray #-}

bDoubleArray :: Storable.Vector Double -> Builder
bDoubleArray =
  bIntArray . coerce
{-# INLINABLE bDoubleArray #-}

getDoubleArray :: Int -> Get (Storable.Vector Double)
getDoubleArray n =
  coerce <$> getIntArray n
{-# INLINABLE getDoubleArray #-}
