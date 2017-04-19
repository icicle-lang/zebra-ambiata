{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Block (
    bBlock
  , getBlock

  , bBlockTable
  , getBlockTable

  -- * Internal
  , bBlockV3
  , getBlockV3

  , bBlockTableV3
  , getBlockTableV3

  , bBlockV2
  , getBlockV2

  , bBlockTableV2
  , getBlockTableV2

  , bEntities
  , getEntities

  , bAttributes
  , getAttributes

  , bIndices
  , getIndices

  , bTables
  , getTables
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           P

import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Factset.Table
import           Zebra.Serial.Binary.Array
import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Table
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


bBlock :: Header -> Block -> Either BinaryEncodeError Builder
bBlock header block =
  case header of
    HeaderV2 _ ->
      bBlockV2 block
    HeaderV3 table -> do
      attributes <- first BinaryEncodeBlockTableError $
        Boxed.fromList . Map.keys <$> attributesOfTableSchema table
      bBlockV3 attributes block
{-# INLINABLE bBlock #-}

getBlock :: Header -> Get Block
getBlock = \case
  HeaderV2 x ->
    getBlockV2 x
  HeaderV3 x ->
    getBlockV3 x
{-# INLINABLE getBlock #-}

bBlockTable :: Header -> Striped.Table -> Either BinaryEncodeError Builder
bBlockTable header table =
  case header of
    HeaderV2 _ ->
      bBlockTableV2 table
    HeaderV3 _ -> do
      bBlockTableV3 table
{-# INLINABLE bBlockTable #-}

getBlockTable :: Header -> Get Striped.Table
getBlockTable = \case
  HeaderV2 x ->
    getBlockTableV2 x
  HeaderV3 x ->
    getBlockTableV3 x
{-# INLINABLE getBlockTable #-}

-- | Encode a zebra v3 block.
--
bBlockV3 :: Boxed.Vector AttributeName -> Block -> Either BinaryEncodeError Builder
bBlockV3 attributes block = do
  table <- first BinaryEncodeBlockTableError $ tableOfBlock attributes block
  bBlockTableV3 table
{-# INLINABLE bBlockV3 #-}

getBlockV3 :: Schema.Table -> Get Block
getBlockV3 schema = do
  table <- getBlockTableV3 schema
  case blockOfTable table of
    Left err ->
      fail . Text.unpack $ renderBlockTableError err
    Right x ->
      pure x
{-# INLINABLE getBlockV3 #-}

bBlockTableV3 :: Striped.Table -> Either BinaryEncodeError Builder
bBlockTableV3 table0 = do
  table <- bTable BinaryV3 table0
  pure $
    Builder.word32LE (fromIntegral $ Striped.length table0) <>
    table
{-# INLINABLE bBlockTableV3 #-}

getBlockTableV3 :: Schema.Table -> Get Striped.Table
getBlockTableV3 schema = do
  n <- fromIntegral <$> Get.getWord32le
  getTable BinaryV3 n schema
{-# INLINABLE getBlockTableV3 #-}

-- | Encode a zebra v2 block.
--
bBlockV2 :: Block -> Either BinaryEncodeError Builder
bBlockV2 block = do
  tables <- bTables (blockTables block)
  pure $
    bEntities (blockEntities block) <>
    bIndices (blockIndices block) <>
    tables
{-# INLINABLE bBlockV2 #-}

getBlockV2 :: Map AttributeName Schema.Column -> Get Block
getBlockV2 schemas = do
  entities <- getEntities
  indices <- getIndices
  tables <- getTables . Boxed.fromList $ Map.elems schemas
  pure $
    Block entities indices tables
{-# INLINABLE getBlockV2 #-}

bBlockTableV2 :: Striped.Table -> Either BinaryEncodeError Builder
bBlockTableV2 =
  bind bBlockV2 . first BinaryEncodeBlockTableError . blockOfTable
{-# INLINABLE bBlockTableV2 #-}

getBlockTableV2 :: Map AttributeName Schema.Column -> Get Striped.Table
getBlockTableV2 schemas = do
  block <- getBlockV2 schemas
  case tableOfBlock (Boxed.fromList $ Map.keys schemas) block of
    Left err ->
      fail . Text.unpack $ renderBlockTableError err
    Right x ->
      pure x
{-# INLINABLE getBlockTableV2 #-}

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
    Builder.word32LE ecount <>
    bIntArray hashes <>
    bStrings ids <>
    bIntArray acounts <>
    bAttributes attributes
{-# INLINABLE bEntities #-}

getEntities :: Get (Boxed.Vector BlockEntity)
getEntities = do
  ecount <- fromIntegral <$> Get.getWord32le
  hashes <- fmap (EntityHash . fromIntegral) . Boxed.convert <$> getIntArray ecount
  ids <- fmap EntityId <$> getStrings ecount
  acounts <- Unboxed.map fromIntegral . Unboxed.convert <$> getIntArray ecount
  attributes <- Generic.unsafeSplits id <$> getAttributes <*> pure acounts
  pure $
    Boxed.zipWith3 BlockEntity hashes ids attributes
{-# INLINABLE getEntities #-}

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
    Builder.word32LE acount <>
    bIntArray ids <>
    bIntArray counts
{-# INLINABLE bAttributes #-}

getAttributes :: Get (Unboxed.Vector BlockAttribute)
getAttributes = do
  acount <- fromIntegral <$> Get.getWord32le
  ids <- Unboxed.map (AttributeId . fromIntegral) . Unboxed.convert <$> getIntArray acount
  counts <- Unboxed.map fromIntegral . Unboxed.convert <$> getIntArray acount
  pure $
    Unboxed.zipWith BlockAttribute ids counts
{-# INLINABLE getAttributes #-}

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
    Builder.word32LE icount <>
    bIntArray times <>
    bIntArray factsetIds <>
    bIntArray tombstones
{-# INLINABLE bIndices #-}

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
{-# INLINABLE getIndices #-}

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
bTables :: Boxed.Vector Striped.Table -> Either BinaryEncodeError Builder
bTables xs0 = do
  let
    n =
      Boxed.length xs0

    tcount =
      fromIntegral n

    ids =
      Storable.map fromIntegral $
      Storable.enumFromTo 0 (n - 1)

    counts =
      Storable.convert $
      fmap (fromIntegral . Striped.length) xs0

  xs <- Boxed.toList <$> traverse (bTable BinaryV2) xs0

  pure $
    Builder.word32LE tcount <>
    bIntArray ids <>
    bIntArray counts <>
    mconcat xs
{-# INLINABLE bTables #-}

getTables :: Boxed.Vector Schema.Column -> Get (Boxed.Vector Striped.Table)
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
          getTable BinaryV2 n (Schema.Array schema)

  Boxed.zipWithM get ids counts
{-# INLINABLE getTables #-}
