{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Binary.Table (
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

import           Zebra.Binary.Array
import           Zebra.Binary.Data
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Schema (TableSchema, ColumnSchema, Tag)
import qualified Zebra.Schema as Schema
import           Zebra.Table (Table, Column)
import qualified Zebra.Table as Table


-- | Encode a zebra table as bytes.
--
bTable :: BinaryVersion -> Table -> Builder
bTable version = \case
  Table.Binary bs ->
    case version of
      BinaryV2 ->
        bSizedByteArray bs
      BinaryV3 ->
        bByteArray bs

  Table.Array c ->
    bColumn version c

  Table.Map k v ->
    bColumn version k <>
    bColumn version v

-- | Encode a zebra column as bytes.
--
bColumn :: BinaryVersion -> Column -> Builder
bColumn version = \case
  Table.Unit _ ->
    mempty

  Table.Int xs ->
    bIntArray xs

  Table.Double xs ->
    bDoubleArray xs

  Table.Enum tags vs ->
    bTagArray tags <>
    foldMap (bColumn version . Schema.variant) vs

  Table.Struct fs ->
    foldMap (bColumn version . Schema.field) fs

  Table.Nested ns x ->
    bIntArray ns <>
    Build.word32LE (fromIntegral $ Table.length x) <>
    bTable version x

  Table.Reversed x ->
    bColumn version x

-- | Decode a zebra table using a row count and a schema.
--
getTable :: BinaryVersion -> Int -> TableSchema -> Get Table
getTable version n = \case
  Schema.Binary ->
    case version of
      BinaryV2 ->
        Table.Binary <$> getSizedByteArray
      BinaryV3 ->
        Table.Binary <$> getByteArray n

  Schema.Array e ->
    Table.Array
      <$> getColumn version n e

  Schema.Map k v ->
    Table.Map
      <$> getColumn version n k
      <*> getColumn version n v

-- | Decode a zebra column using a row count and a schema.
--
getColumn :: BinaryVersion -> Int -> ColumnSchema -> Get Column
getColumn version n = \case
  Schema.Unit ->
    pure $ Table.Unit n

  Schema.Int ->
    Table.Int
      <$> getIntArray n

  Schema.Double ->
    Table.Double
      <$> getDoubleArray n

  Schema.Enum vs ->
    Table.Enum
      <$> getTagArray n
      <*> Cons.mapM (traverse $ getColumn version n) vs

  Schema.Struct fs ->
    Table.Struct
      <$> Cons.mapM (traverse $ getColumn version n) fs

  Schema.Nested t ->
    Table.Nested
      <$> getIntArray n
      <*> (flip (getTable version) t . fromIntegral =<< Get.getWord32le)

  Schema.Reversed c ->
    Table.Reversed
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
