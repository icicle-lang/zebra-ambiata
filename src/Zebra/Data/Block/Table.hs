{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block.Table (
    BlockTableError(..)
  , renderBlockTableError

  , tableOfBlock
  , blockOfTable

  , tableSchemaOfAttributes
  , attributesOfTableSchema
  ) where

import qualified Data.ByteString as ByteString
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Text as Text

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable
import           X.Data.Vector.Unboxed (Unbox)
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Block.Block
import           Zebra.Data.Block.Entity
import           Zebra.Data.Block.Index
import           Zebra.Data.Core
import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import qualified Zebra.Data.Vector.Generic as Generic
import           Zebra.Schema (SchemaError, TableSchema, ColumnSchema, Field(..), FieldName(..), Variant(..), Tag(..))
import qualified Zebra.Schema as Schema
import           Zebra.Segment (SegmentError)
import qualified Zebra.Segment as Segment
import           Zebra.Table (Table, Column)
import qualified Zebra.Table as Table

------------------------------------------------------------------------

data BlockTableError =
    BlockAttributeNamesDidNotMatchTableCount !Int !(Boxed.Vector AttributeName)
  | BlockTableSchemaError !SchemaError
  | BlockEntityIdLengthMismatch !SegmentError
  | BlockIndexLengthMismatch !SegmentError
  | BlockExpectedEntityFields !(Cons Boxed.Vector (Field ColumnSchema))
  | BlockExpectedIndexFields !(Cons Boxed.Vector (Field ColumnSchema))
  | BlockExpectedAttributes !ColumnSchema
  | BlockExpectedOption !ColumnSchema
    deriving (Eq, Ord, Show)

renderBlockTableError :: BlockTableError -> Text
renderBlockTableError = \case
  BlockAttributeNamesDidNotMatchTableCount n attrs ->
    "Expected " <> Text.pack (show n) <> " attributes, but found: " <> Text.pack (show attrs)
  BlockTableSchemaError err ->
    Schema.renderSchemaError err
  BlockEntityIdLengthMismatch err ->
    "Error decoding entity-ids: " <> Segment.renderSegmentError err
  BlockIndexLengthMismatch err ->
    "Error decoding indices: " <> Segment.renderSegmentError err
  BlockExpectedEntityFields fields ->
    "Expected struct with fields <entity_hash, entity_id>, but found: " <> Text.pack (show fields)
  BlockExpectedIndexFields fields ->
    "Expected struct with fields <time, factset_id>, but found: " <> Text.pack (show fields)
  BlockExpectedAttributes schema ->
    "Expected a struct containing the attributes, but found: " <> Text.pack (show schema)
  BlockExpectedOption schema ->
    "Expected an option type, representing tombstones, but found: " <> Text.pack (show schema)

------------------------------------------------------------------------
-- Block -> Table

entityHashColumn :: Boxed.Vector BlockEntity -> Column
entityHashColumn =
  Table.Int . Storable.convert . fmap (fromIntegral . unEntityHash . entityHash)

entityIdColumn :: Boxed.Vector BlockEntity -> Column
entityIdColumn xs =
  let
    ns =
      Storable.convert $
      fmap (fromIntegral . ByteString.length . unEntityId . entityId) xs

    bytes =
      Table.Binary .
      ByteString.concat .
      Boxed.toList $
      fmap (unEntityId . entityId) xs
  in
    Table.Nested ns bytes

entityColumn :: Boxed.Vector BlockEntity -> Column
entityColumn xs =
  Table.Struct $ Cons.from2
    (Field "entity_hash" $ entityHashColumn xs)
    (Field "entity_id" $ entityIdColumn xs)

replicateAttributeId :: Boxed.Vector BlockEntity -> Unboxed.Vector AttributeId
replicateAttributeId =
  Unboxed.concatMap (\x -> Unboxed.replicate (fromIntegral $ attributeRows x) (attributeId x)) .
  Unboxed.convert .
  Boxed.concatMap (Boxed.convert . entityAttributes)
{-# INLINE replicateAttributeId #-}

takeRowCounts :: AttributeId -> Boxed.Vector BlockEntity -> Unboxed.Vector Int64
takeRowCounts aid =
  let
    rowsOrZero =
      maybe 0 attributeRows .
      Unboxed.find (\a -> attributeId a == aid) .
      entityAttributes
  in
    Unboxed.convert . Boxed.map rowsOrZero
{-# INLINE takeRowCounts #-}

distributeIndices ::
  (Unboxed.Vector BlockIndex -> Unboxed.Vector Int64 -> Column -> a) ->
  Boxed.Vector BlockEntity ->
  Unboxed.Vector BlockIndex ->
  Cons Boxed.Vector (Field Column) ->
  Cons Boxed.Vector (Field a)
distributeIndices done0 entities indices columns =
  Cons.ifor columns $ \needle0 field0 ->
    let
      needle =
        AttributeId $ fromIntegral needle0

      done ixs =
        fmap (done0 ixs (takeRowCounts needle entities)) field0
    in
      done .
      Unboxed.map snd .
      Unboxed.filter (\(aid, _) -> aid == needle) $
      Unboxed.zip (replicateAttributeId entities) indices
{-# INLINE distributeIndices #-}

timeColumn :: Unboxed.Vector BlockIndex -> Column
timeColumn =
  Table.Int . Storable.convert . Unboxed.map (unTime . indexTime)

factsetColumn :: Unboxed.Vector BlockIndex -> Column
factsetColumn =
  Table.Reversed . Table.Int . Storable.convert . Unboxed.map (unFactsetId . indexFactsetId)

tombstoneTags :: Unboxed.Vector BlockIndex -> Storable.Vector Tag
tombstoneTags =
  Storable.map fromTombstone . Storable.convert . Unboxed.map indexTombstone

fromTombstone :: Tombstone -> Tag
fromTombstone = \case
  Tombstone ->
    0
  NotTombstone ->
    1

attributeTable :: Unboxed.Vector BlockIndex -> Unboxed.Vector Int64 -> Column -> Column
attributeTable indices0 counts values =
  let
    (key_counts, (value_counts, indices)) =
      second Unboxed.unzip $
        Generic.segmentedGroup (Unboxed.map fromIntegral counts) indices0

    key =
      Table.Struct $ Cons.from2
        (Field "time" $ timeColumn indices)
        (Field "factset_id" $ factsetColumn indices)

    nested =
      Table.Nested (Storable.convert $ Unboxed.map fromIntegral value_counts) .
      Table.Array

    value =
      Table.Enum (tombstoneTags indices0) $ Cons.from2
        (Variant "none" . Table.Unit $ Unboxed.length indices0)
        (Variant "some" values)
  in
    Table.Nested
      (Storable.convert $ Unboxed.map fromIntegral key_counts)
      (Table.Map key (nested value))

blockAttributes ::
  Boxed.Vector AttributeName ->
  Block ->
  Either BlockTableError (Maybe (Cons Boxed.Vector (Field Column)))
blockAttributes names block = do
  let
    tables =
      blockTables block

    n_tables =
      Boxed.length tables

    n_names =
      Boxed.length names

    mkField n =
      Field (FieldName $ unAttributeName n)

  columns <- first BlockTableSchemaError $ traverse Table.takeArray tables

  if n_names /= n_tables then
    Left $ BlockAttributeNamesDidNotMatchTableCount n_tables names
  else
    pure . Cons.fromVector $ Boxed.zipWith mkField names columns

tableOfBlock :: Boxed.Vector AttributeName -> Block -> Either BlockTableError Table
tableOfBlock names block = do
  let
    entities =
      blockEntities block

    indices =
      blockIndices block

  mfields <- blockAttributes names block

  case mfields of
    Nothing ->
      pure $ Table.Map
        (entityColumn entities)
        (Table.Unit $ Boxed.length entities)

    Just fields ->
      pure $ Table.Map
        (entityColumn entities)
        (Table.Struct $ distributeIndices attributeTable entities indices fields)

------------------------------------------------------------------------
-- Table -> Block

takeEntityHash :: Column -> Either BlockTableError (Boxed.Vector EntityHash)
takeEntityHash =
  first BlockTableSchemaError .
  fmap (fmap (EntityHash . fromIntegral) . Boxed.convert) .
  Table.takeInt

takeEntityId :: Column -> Either BlockTableError (Boxed.Vector EntityId)
takeEntityId nested = do
  (ns, bytes0) <- first BlockTableSchemaError $ Table.takeNested nested
  bytes <- first BlockTableSchemaError $ Table.takeBinary bytes0

  first BlockEntityIdLengthMismatch $
    fmap EntityId <$> Segment.reify ns bytes

takeEntityKey :: Column -> Either BlockTableError (Boxed.Vector (EntityHash, EntityId))
takeEntityKey column = do
  fields <- first BlockTableSchemaError $ Table.takeStruct column
  case Cons.toList fields of
    [Field "entity_hash" ehash, Field "entity_id" eid] ->
      Boxed.zip
        <$> takeEntityHash ehash
        <*> takeEntityId eid
    _ ->
      Left $ BlockExpectedEntityFields (fmap (fmap Table.schemaColumn) fields)

takeAttributeRowCount :: Column -> Either BlockTableError (Unboxed.Vector Int64)
takeAttributeRowCount column = do
  (k_counts, table) <- first BlockTableSchemaError $ Table.takeNested column
  (_k, v) <- first BlockTableSchemaError $ Table.takeMap table
  (v_counts, _) <- first BlockTableSchemaError $ Table.takeNested v
  first BlockIndexLengthMismatch . fmap (Unboxed.convert . fmap Storable.sum) $
    Segment.reify k_counts v_counts

fromDenseRowCounts :: Unboxed.Vector Int64 -> Unboxed.Vector BlockAttribute
fromDenseRowCounts =
  let
    mk i n =
      BlockAttribute (AttributeId $ fromIntegral i) n
  in
    Unboxed.filter (\x -> attributeRows x /= 0) .
    Unboxed.imap mk

takeAttributes :: Column -> Either BlockTableError (Boxed.Vector (Field Column))
takeAttributes = \case
  Table.Unit _ ->
    pure Boxed.empty
  Table.Struct fields ->
    pure $ Cons.toVector fields
  x ->
    Left $ BlockExpectedAttributes (Table.schemaColumn x)

takeAttributeRowCounts :: Column -> Either BlockTableError (Boxed.Vector (Unboxed.Vector BlockAttribute))
takeAttributeRowCounts column = do
  attrs <- takeAttributes column
  fmap (fmap fromDenseRowCounts . Generic.transpose) $
    traverse (takeAttributeRowCount . field) attrs

takeEntities :: Column -> Column -> Either BlockTableError (Boxed.Vector BlockEntity)
takeEntities key0 value0 = do
  key <- takeEntityKey key0
  counts <- takeAttributeRowCounts value0
  pure $ Boxed.zipWith (uncurry BlockEntity) key counts

takeTime :: Column -> Either BlockTableError (Unboxed.Vector Time)
takeTime =
  fmap (Unboxed.map Time . Unboxed.convert) . first BlockTableSchemaError . Table.takeInt

takeFactsetId :: Column -> Either BlockTableError (Unboxed.Vector FactsetId)
takeFactsetId column0 = do
  column <- first BlockTableSchemaError $ Table.takeReversed column0
  fmap (Unboxed.map FactsetId . Unboxed.convert) . first BlockTableSchemaError $ Table.takeInt column

takeIndexKey :: Column -> Either BlockTableError (Unboxed.Vector  (Time, FactsetId))
takeIndexKey column = do
  fields <- first BlockTableSchemaError $ Table.takeStruct column
  case Cons.toList fields of
    [Field "time" time, Field "factset_id" fid] ->
      Unboxed.zip
        <$> takeTime time
        <*> takeFactsetId fid
    _ ->
      Left $ BlockExpectedIndexFields (fmap (fmap Table.schemaColumn) fields)

fromTag :: Tag -> Tombstone
fromTag = \case
  1 ->
    NotTombstone
  _ ->
    Tombstone

takeTombstone :: Column -> Either BlockTableError (Storable.Vector Int64, Unboxed.Vector Tombstone)
takeTombstone nested = do
  (counts, array) <- first BlockTableSchemaError $ Table.takeNested nested
  enum <- first BlockTableSchemaError $ Table.takeArray array
  column <- fmap fst . first BlockTableSchemaError $ Table.takeEnum enum
  pure (counts, Unboxed.convert $ Storable.map fromTag column)

replicates :: Unbox a => Storable.Vector Int64 -> Unboxed.Vector a -> Unboxed.Vector a
replicates ns xs =
  Unboxed.concatMap (uncurry Unboxed.replicate) $
  Unboxed.zip (Unboxed.map fromIntegral $ Storable.convert ns) xs

takeIndex :: Column -> Either BlockTableError (Boxed.Vector (Unboxed.Vector BlockIndex))
takeIndex column = do
  (k_counts, table) <- first BlockTableSchemaError $ Table.takeNested column
  (k, v) <- first BlockTableSchemaError $ Table.takeMap table

  (v_counts, tombstones) <- takeTombstone v
  ikey <- replicates v_counts <$> takeIndexKey k

  let
    indices =
      Unboxed.zipWith (uncurry BlockIndex) ikey tombstones

  kv_counts <- first BlockIndexLengthMismatch . fmap (fmap Storable.sum) $ Segment.reify k_counts v_counts

  first BlockIndexLengthMismatch $
    Segment.reify kv_counts indices

takeIndices :: Column -> Either BlockTableError (Unboxed.Vector BlockIndex)
takeIndices column = do
  attrs <- takeAttributes column
  indices <- traverse (takeIndex . field) attrs
  pure .
    Unboxed.convert .
    Boxed.concatMap (Boxed.concatMap Boxed.convert) $
    Generic.transpose indices

takeTable :: Column -> Either BlockTableError Table
takeTable column0 = do
  (_ns, table0) <- first BlockTableSchemaError $ Table.takeNested column0
  (_, value) <- first BlockTableSchemaError $ Table.takeMap table0
  (_1s, table) <- first BlockTableSchemaError $ Table.takeNested value
  array <- first BlockTableSchemaError $ Table.takeArray table
  (_tag, e) <- first BlockTableSchemaError $ Table.takeEnum array
  case Cons.toList e of
    [Variant "none" _, Variant "some" c] ->
      pure $ Table.Array c
    _ ->
      Left $ BlockExpectedOption (Table.schemaColumn array)

takeTables :: Column -> Either BlockTableError (Boxed.Vector Table)
takeTables column = do
  attrs <- takeAttributes column
  traverse (takeTable . field) attrs

blockOfTable :: Table -> Either BlockTableError Block
blockOfTable table = do
  (k, v) <- first BlockTableSchemaError $ Table.takeMap table
  entities <- takeEntities k v
  indices <- takeIndices v
  tables <- takeTables v
  pure $ Block entities indices tables

------------------------------------------------------------------------
-- TableSchema -> Map AttributeName TableSchema

fromAttribute :: AttributeName -> ColumnSchema -> Field ColumnSchema
fromAttribute (AttributeName name) column =
  Field (FieldName name) . Schema.Nested $
    Schema.Map
      (Schema.Struct $ Cons.from2
        (Field "time" Schema.Int)
        (Field "factset_id" $ Schema.Reversed Schema.Int))
      (Schema.Nested . Schema.Array $ Schema.option column)

fromFields :: Boxed.Vector (Field ColumnSchema) -> ColumnSchema
fromFields xs0 =
  case Cons.fromVector xs0 of
    Nothing ->
      Schema.Unit
    Just xs ->
      Schema.Struct xs

tableSchemaOfAttributes :: Map AttributeName ColumnSchema -> TableSchema
tableSchemaOfAttributes attrs0 =
  let
    attrs =
      fromFields .
      Boxed.fromList .
      fmap (uncurry fromAttribute) $
      Map.toList attrs0
  in
    Schema.Map
      (Schema.Struct $ Cons.from2
        (Field "entity_hash" Schema.Int)
        (Field "entity_id" $ Schema.Nested Schema.Binary))
      attrs

------------------------------------------------------------------------
-- Map AttributeName TableSchema -> TableSchema

takeAttribute :: Field ColumnSchema -> Either BlockTableError (AttributeName, ColumnSchema)
takeAttribute (Field (FieldName name) column) = do
  kv_table <- first BlockTableSchemaError $ Schema.takeNested column
  (k, v) <- first BlockTableSchemaError $ Schema.takeMap kv_table
  k_fields <- first BlockTableSchemaError $ Schema.takeStruct k
  case Cons.toList k_fields of
    [Field "time" Schema.Int, Field "factset_id" (Schema.Reversed Schema.Int)] -> do
      v_table <- first BlockTableSchemaError $ Schema.takeNested v
      v_array <- first BlockTableSchemaError $ Schema.takeArray v_table
      v_some <- first BlockTableSchemaError $ Schema.takeOption v_array
      pure $ (AttributeName name, v_some)
    _ ->
      Left $ BlockExpectedIndexFields k_fields

takeFields :: ColumnSchema -> Either BlockTableError (Boxed.Vector (Field ColumnSchema))
takeFields = \case
  Schema.Unit ->
    pure Boxed.empty
  Schema.Struct fs ->
    pure $ Cons.toVector fs
  x ->
    Left $ BlockExpectedAttributes x

attributesOfTableSchema :: TableSchema -> Either BlockTableError (Map AttributeName ColumnSchema)
attributesOfTableSchema table = do
  (k, v) <- first BlockTableSchemaError $ Schema.takeMap table
  k_fields <- first BlockTableSchemaError $ Schema.takeStruct k
  case Cons.toList k_fields of
    [Field "entity_hash" Schema.Int, Field "entity_id" (Schema.Nested Schema.Binary)] -> do
      v_fields <- takeFields v
      attrs <- traverse takeAttribute v_fields
      pure . Map.fromList $ Boxed.toList attrs
    _ ->
      Left $ BlockExpectedEntityFields k_fields
