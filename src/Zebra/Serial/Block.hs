{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Block (
    bBlock
  , getBlock

  , bEntities
  , getEntities

  , bAttributes
  , getAttributes

  , bIndices
  , getIndices

  , bTables
  , bTable
  , bColumn
  , getTables
  , getTable
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import           Data.Coerce (coerce)
import qualified Data.Vector as Boxed

import           P

import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Schema (Schema)
import qualified Zebra.Schema as Schema
import           Zebra.Serial.Array
import           Zebra.Table (Table(..), Column(..))
import qualified Zebra.Table as Table


-- | Encode a zebra block.
--
bBlock :: Block a -> Builder
bBlock block =
  bEntities (blockEntities block) <>
  bIndices (blockIndices block) <>
  bTables (blockTables block)

getBlock :: Boxed.Vector Schema -> Get (Block Schema)
getBlock schemas = do
  entities <- getEntities
  indices <- getIndices
  tables <- getTables schemas
  pure $
    Block entities indices tables

-- | Encode the entities for a zebra block.
--
--   Entities are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   entity {
--     hash    : int
--     id      : string
--     n_attrs : int
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   entity_count      : u32
--   entity_id_hash    : int_array entity_count
--   entity_id_length  : int_array entity_count
--   entity_id_string  : byte_array
--   entity_attr_count : int_array entity_count
-- @
--
--   Entities are then followed by the attributes for the block, see
--   'bAttributes' for the format.
--
bEntities :: Boxed.Vector BlockEntity -> Builder
bEntities xs =
  let
    ecount =
      fromIntegral $ Boxed.length xs

    hashes =
      Boxed.convert $ fmap (fromIntegral . unEntityHash . entityHash) xs

    ids =
      fmap (unEntityId . entityId) xs

    acounts =
      Boxed.convert $ fmap (fromIntegral . Unboxed.length . entityAttributes) xs

    attributes =
      Stream.vectorOfStream $
      Stream.concatMap (Stream.streamOfVector . entityAttributes) $
      Stream.streamOfVector xs
  in
    Build.word32LE ecount <>
    bIntArray hashes <>
    bStrings ids <>
    bIntArray acounts <>
    bAttributes attributes

getEntities :: Get (Boxed.Vector BlockEntity)
getEntities = do
  ecount <- fromIntegral <$> Get.getWord32le
  hashes <- fmap (EntityHash . fromIntegral) . Boxed.convert <$> getIntArray ecount
  ids <- fmap EntityId <$> getStrings ecount
  acounts <- Unboxed.map fromIntegral . Unboxed.convert <$> getIntArray ecount
  attributes <- Generic.unsafeSplits id <$> getAttributes <*> pure acounts
  pure $
    Boxed.zipWith3 BlockEntity hashes ids attributes

-- | Encode the attributes for a zebra block.
--
--   Attributes are encoded as a data flattened version of the following
--   logical structure:
--
-- @
--   attr {
--     id    : int
--     count : int
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   attr_count    : u32
--   attr_id       : int_array attr_count
--   attr_id_count : int_array attr_count
-- @
--
--   /invariant: attr_count == sum entity_attr_count/
--   /invariant: attr_ids are sorted for each entity/
--
bAttributes :: Unboxed.Vector BlockAttribute -> Builder
bAttributes xs =
  let
    acount =
      fromIntegral $
      Storable.length ids

    ids =
      Unboxed.convert $
      Unboxed.map (fromIntegral . unAttributeId . attributeId) xs

    counts =
      Unboxed.convert $
      Unboxed.map (fromIntegral . attributeRows) xs
  in
    Build.word32LE acount <>
    bIntArray ids <>
    bIntArray counts

getAttributes :: Get (Unboxed.Vector BlockAttribute)
getAttributes = do
  acount <- fromIntegral <$> Get.getWord32le
  ids <- Unboxed.map (AttributeId . fromIntegral) . Unboxed.convert <$> getIntArray acount
  counts <- Unboxed.map fromIntegral . Unboxed.convert <$> getIntArray acount
  pure $
    Unboxed.zipWith BlockAttribute ids counts

-- | Encode the table index for a zebra block.
--
--   Indices are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   index {
--     time         : int
--     factset_id   : int
--     is_tombstone : int
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   index_count      : u32
--   index_time       : int_array value_count
--   index_factset_id : int_array value_count
--   index_tombstone  : int_array value_count
-- @
--
--   /invariant: index_count == sum attr_id_count/
--
bIndices :: Unboxed.Vector BlockIndex -> Builder
bIndices xs =
  let
    icount =
      fromIntegral $
      Unboxed.length xs

    times =
      Unboxed.convert $
      Unboxed.map (unTime . indexTime) xs

    factsetIds =
      Unboxed.convert $
      Unboxed.map (unFactsetId . indexFactsetId) xs

    tombstones =
      Unboxed.convert $
      Unboxed.map (foreignOfTombstone . indexTombstone) xs
  in
    Build.word32LE icount <>
    bIntArray times <>
    bIntArray factsetIds <>
    bIntArray tombstones

getIndices :: Get (Unboxed.Vector BlockIndex)
getIndices = do
  icount <- fromIntegral <$> Get.getWord32le
  itimes <- getIntArray icount
  ifactsetIds <- getIntArray icount
  itombstones <- getIntArray icount

  let
    times =
      Unboxed.map Time $
      Unboxed.convert itimes

    factsetIds =
      Unboxed.map FactsetId $
      Unboxed.convert ifactsetIds

    tombstones =
      Unboxed.map tombstoneOfForeign $
      Unboxed.convert itombstones

  pure $ Unboxed.zipWith3 BlockIndex times factsetIds tombstones

-- | Encode the table data for a zebra block.
--
--   Tables are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   table {
--     attr_id   : int
--     row_count : int
--     data      : array of ?
--   }
-- @
--
--   'table_data' contains flattened arrays of values, exactly how many arrays
--   and what format is described by the format in the header.
--
-- @
--   table_count     : u32
--   table_id        : int_array table_count
--   table_row_count : int_array table_count
--   table_data      : ?
-- @
--
--   /invariant: table_count == count of unique attr_ids/
--   /invariant: table_id contains all ids referenced by attr_ids/
--
bTables :: Boxed.Vector (Table a) -> Builder
bTables xs =
  let
    n =
      Boxed.length xs

    tcount =
      fromIntegral n

    ids =
      Storable.map fromIntegral $
      Storable.enumFromTo 0 (n - 1)

    counts =
      Storable.convert $
      fmap (fromIntegral . Table.rowCount) xs
  in
    Build.word32LE tcount <>
    bIntArray ids <>
    bIntArray counts <>
    foldMap bTable xs

getTables :: Boxed.Vector Schema -> Get (Boxed.Vector (Table Schema))
getTables schemas = do
  tcount <- fromIntegral <$> Get.getWord32le
  ids <- fmap fromIntegral . Boxed.convert <$> getIntArray tcount
  counts <- fmap fromIntegral . Boxed.convert <$> getIntArray tcount

  let
    get aid n =
      case schemas Boxed.!? aid of
        Nothing ->
          fail $ "Cannot read table, unknown attribute-id: " <> show aid
        Just schema ->
          getTable n schema

  Boxed.zipWithM get ids counts

bTable :: Table a -> Builder
bTable =
  foldMap bColumn . Table.columns

bColumn :: Column a -> Builder
bColumn = \case
  ByteColumn bs ->
    bByteArray bs
  IntColumn xs ->
    bIntArray xs
  DoubleColumn xs ->
    bIntArray $ coerce xs
  ArrayColumn ns rec ->
    bIntArray ns <>
    Build.word32LE (fromIntegral $ Table.rowCount rec) <>
    bTable rec

getTable :: Int -> Schema -> Get (Table Schema)
getTable n schema =
  case schema of
    Schema.Byte ->
      Table schema n . Boxed.singleton <$> getByteColumn n

    Schema.Int ->
      Table schema n . Boxed.singleton <$> getIntColumn n

    Schema.Double ->
      Table schema n . Boxed.singleton <$> getDoubleColumn n

    Schema.Enum variant0 variants -> do
      tags <- getIntColumn n
      cols <- concatMapM (fmap Table.columns . getTable n . Schema.variantSchema) (Boxed.cons variant0 variants)
      pure . Table schema n $ Boxed.cons tags cols

    Schema.Struct fields -> do
      cols <- concatMapM (fmap Table.columns . getTable n . Schema.fieldSchema) fields
      pure $ Table schema n cols

    Schema.Array element -> do
      counts <- getIntArray n
      total <- fromIntegral <$> Get.getWord32le
      inner <- getTable total element
      pure . Table schema n . Boxed.singleton $
        ArrayColumn counts inner

getByteColumn :: Int -> Get (Column a)
getByteColumn _n =
  ByteColumn <$> getByteArray

getIntColumn :: Int -> Get (Column a)
getIntColumn n =
  IntColumn <$> getIntArray n

getDoubleColumn :: Int -> Get (Column a)
getDoubleColumn n =
  DoubleColumn . coerce <$> getIntArray n

concatMapM :: Monad m => (a -> m (Boxed.Vector b)) -> Boxed.Vector a -> m (Boxed.Vector b)
concatMapM f =
  Stream.vectorOfStreamM .
  Stream.concatMapM (fmap Stream.streamOfVectorM . f) .
  Stream.streamOfVectorM
{-# INLINE concatMapM #-}
