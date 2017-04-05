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
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import           Data.Coerce (coerce)

import           P

import qualified X.Data.Vector.Storable as Storable

import           Zebra.Serial.Binary.Array
import           Zebra.Serial.Binary.Data
import           Zebra.Table.Schema (TableSchema, ColumnSchema, Tag)
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.X.Vector.Cons as Cons


-- | Encode a zebra table as bytes.
--
bTable :: BinaryVersion -> Striped.Table -> Builder
bTable version = \case
  Striped.Binary bs ->
    case version of
      BinaryV2 ->
        bSizedByteArray bs
      BinaryV3 ->
        bByteArray bs

  Striped.Array c ->
    bColumn version c

  Striped.Map k v ->
    bColumn version k <>
    bColumn version v

-- | Encode a zebra column as bytes.
--
bColumn :: BinaryVersion -> Striped.Column -> Builder
bColumn version = \case
  Striped.Unit _ ->
    mempty

  Striped.Int xs ->
    bIntArray xs

  Striped.Double xs ->
    bDoubleArray xs

  Striped.Enum tags vs ->
    bTagArray tags <>
    foldMap (bColumn version . Schema.variant) vs

  Striped.Struct fs ->
    foldMap (bColumn version . Schema.field) fs

  Striped.Nested ns x ->
    bIntArray ns <>
    Build.word32LE (fromIntegral $ Striped.length x) <>
    bTable version x

  Striped.Reversed x ->
    bColumn version x

-- | Decode a zebra table using a row count and a schema.
--
getTable :: BinaryVersion -> Int -> TableSchema -> Get Striped.Table
getTable version n = \case
  Schema.Binary ->
    case version of
      BinaryV2 ->
        Striped.Binary <$> getSizedByteArray
      BinaryV3 ->
        Striped.Binary <$> getByteArray n

  Schema.Array e ->
    Striped.Array
      <$> getColumn version n e

  Schema.Map k v ->
    Striped.Map
      <$> getColumn version n k
      <*> getColumn version n v

-- | Decode a zebra column using a row count and a schema.
--
getColumn :: BinaryVersion -> Int -> ColumnSchema -> Get Striped.Column
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
