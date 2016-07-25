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

  , bRecords
  , bRecord
  , bField
  , getRecords
  , getRecord
  , getField
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
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Data.Index
import           Zebra.Data.Record
import           Zebra.Data.Schema
import           Zebra.Serial.Array


-- | Encode a zebra block.
--
bBlock :: Block -> Builder
bBlock block =
  bEntities (blockEntities block) <>
  bIndices (blockIndices block) <>
  bRecords (blockRecords block)

getBlock :: Boxed.Vector Schema -> Get Block
getBlock schemas = do
  entities <- getEntities
  indices <- getIndices
  records <- getRecords schemas
  pure $
    Block entities indices records

-- | Encode the entities for a zebra block.
--
--   Entities are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   entity {
--     hash    : word
--     id      : string
--     n_attrs : word
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   entity_count      : u32
--   entity_id_hash    : word_array entity_count
--   entity_id_length  : word_array entity_count
--   entity_id_string  : byte_array
--   entity_attr_count : word_array entity_count
-- @
--
--   Entities are then followed by the attributes for the block, see
--   'bAttributes' for the format.
--
bEntities :: Boxed.Vector Entity -> Builder
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
    bWordArray hashes <>
    bStrings ids <>
    bWordArray acounts <>
    bAttributes attributes

getEntities :: Get (Boxed.Vector Entity)
getEntities = do
  ecount <- fromIntegral <$> Get.getWord32le
  hashes <- fmap (EntityHash . fromIntegral) . Boxed.convert <$> getWordArray ecount
  ids <- fmap EntityId <$> getStrings ecount
  acounts <- Unboxed.map fromIntegral . Unboxed.convert <$> getWordArray ecount
  attributes <- Generic.unsafeSplits id <$> getAttributes <*> pure acounts
  pure $
    Boxed.zipWith3 Entity hashes ids attributes

-- | Encode the attributes for a zebra block.
--
--   Attributes are encoded as a data flattened version of the following
--   logical structure:
--
-- @
--   attr {
--     id    : word
--     count : word
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   attr_count    : u32
--   attr_id       : word_array attr_count
--   attr_id_count : word_array attr_count
-- @
--
--   /invariant: attr_count == sum entity_attr_count/
--   /invariant: attr_ids are sorted for each entity/
--
bAttributes :: Unboxed.Vector Attribute -> Builder
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
      Unboxed.map (fromIntegral . attributeRecords) xs
  in
    Build.word32LE acount <>
    bWordArray ids <>
    bWordArray counts

getAttributes :: Get (Unboxed.Vector Attribute)
getAttributes = do
  acount <- fromIntegral <$> Get.getWord32le
  ids <- Unboxed.map (AttributeId . fromIntegral) . Unboxed.convert <$> getWordArray acount
  counts <- Unboxed.map fromIntegral . Unboxed.convert <$> getWordArray acount
  pure $
    Unboxed.zipWith Attribute ids counts

-- | Encode the record index for a zebra block.
--
--   Indices are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   index {
--     time         : word
--     priority     : word
--     is_tombstone : word
--   }
-- @
--
--   The physical array structure is as follows:
--
-- @
--   index_count      : u32
--   index_time_epoch : u64
--   index_time_delta : word_array value_count
--   index_priority   : word_array value_count
--   index_tombstone  : word_array value_count
-- @
--
--   /invariant: index_count == sum attr_id_count/
--
bIndices :: Unboxed.Vector Index -> Builder
bIndices xs =
  let
    icount =
      fromIntegral $
      Unboxed.length xs

    iepoch =
      if Unboxed.null xs then
        0
      else
        unTime .
        Unboxed.minimum $
        Unboxed.map indexTime xs

    deltas =
      Unboxed.convert $
      Unboxed.map (fromIntegral . subtract iepoch . unTime . indexTime) xs

    priorities =
      Unboxed.convert $
      Unboxed.map (fromIntegral . unPriority . indexPriority) xs

    tombstones =
      Unboxed.convert $
      Unboxed.map (fromIntegral . wordOfTombstone . indexTombstone) xs
  in
    Build.word32LE icount <>
    Build.int64LE iepoch <>
    bWordArray deltas <>
    bWordArray priorities <>
    bWordArray tombstones

getIndices :: Get (Unboxed.Vector Index)
getIndices = do
  icount <- fromIntegral <$> Get.getWord32le
  iepoch <- fromIntegral <$> Get.getWord64le
  wdeltas <- getWordArray icount
  wpriorities <- getWordArray icount
  wtombstones <- getWordArray icount

  let
    times =
      Unboxed.map (Time . (+ iepoch) . fromIntegral) $
      Unboxed.convert wdeltas

    priorities =
      Unboxed.map (Priority . fromIntegral) $
      Unboxed.convert wpriorities

    tombstones =
      Unboxed.map (tombstoneOfWord . fromIntegral) $
      Unboxed.convert wtombstones

  pure $ Unboxed.zipWith3 Index times priorities tombstones

-- | Encode the record data for a zebra block.
--
--   Records are encoded as a data flattened version of the following logical
--   structure:
--
-- @
--   record {
--     attr_id : word
--     count   : word
--     size    : word
--     data    : array of ?
--   }
-- @
--
--   'record_data' contains flattened arrays of values, exactly how many arrays
--   and what format is described by the schema in the header.
--
-- @
--   record_count    : u32
--   record_id       : word_array record_count
--   record_id_count : word_array record_count
--   record_data     : ?
-- @
--
--   /invariant: record_count == count of unique attr_ids/
--   /invariant: record_id contains all ids referenced by attr_ids/
--
bRecords :: Boxed.Vector Record -> Builder
bRecords xs =
  let
    n =
      Boxed.length xs

    rcount =
      fromIntegral n

    ids =
      Storable.map fromIntegral $
      Storable.enumFromTo 0 (n - 1)

    counts =
      Storable.convert $
      fmap (fromIntegral . lengthOfRecord) xs
  in
    Build.word32LE rcount <>
    bWordArray ids <>
    bWordArray counts <>
    foldMap bRecord xs

getRecords :: Boxed.Vector Schema -> Get (Boxed.Vector Record)
getRecords schemas = do
  rcount <- fromIntegral <$> Get.getWord32le
  ids <- fmap fromIntegral . Boxed.convert <$> getWordArray rcount
  counts <- fmap fromIntegral . Boxed.convert <$> getWordArray rcount

  let
    get aid n =
      case schemas Boxed.!? aid of
        Nothing ->
          fail $ "Cannot read record, unknown attribute-id: " <> show aid
        Just schema ->
          getRecord n schema

  Boxed.zipWithM get ids counts

bRecord :: Record -> Builder
bRecord =
  foldMap bField . recordFields

getRecord :: Int -> Schema -> Get Record
getRecord n (Schema fmts) = do
  Record . Boxed.fromList <$> traverse (getField n) fmts

bField :: Field -> Builder
bField = \case
  ByteField bs ->
    bByteArray bs
  WordField xs ->
    bWordArray xs
  DoubleField xs ->
    bWordArray $ coerce xs
  ListField ns rec ->
    bWordArray ns <>
    Build.word32LE (fromIntegral $ lengthOfRecord rec) <>
    bRecord rec

getField :: Int -> Format -> Get Field
getField n = \case
  ByteFormat ->
    ByteField <$> getByteArray
  WordFormat ->
    WordField <$> getWordArray n
  DoubleFormat ->
    DoubleField . coerce <$> getWordArray n
  ListFormat schema -> do
    ns <- getWordArray n
    m <- Get.getWord32le
    rec <- getRecord (fromIntegral m) schema
    pure $
      ListField ns rec
