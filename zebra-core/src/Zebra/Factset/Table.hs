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
module Zebra.Factset.Table (
    BlockTableError(..)
  , renderBlockTableError

  , tableOfBlock
  , blockOfTable

  , tableSchemaOfAttributes
  , attributesOfTableSchema
  ) where

import qualified Data.ByteString as ByteString
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable
import           X.Data.Vector.Unboxed (Unbox)
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Factset.Block.Block
import           Zebra.Factset.Block.Entity
import           Zebra.Factset.Block.Index
import           Zebra.Factset.Data
import           Zebra.Table.Data
import           Zebra.Table.Schema (SchemaError)
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons
import qualified Zebra.X.Vector.Generic as Generic
import           Zebra.X.Vector.Segment (SegmentError)
import qualified Zebra.X.Vector.Segment as Segment

------------------------------------------------------------------------

data BlockTableError =
    BlockAttributeNamesDidNotMatchTableCount !Int !(Boxed.Vector AttributeName)
  | BlockTableSchemaError !SchemaError
  | BlockEntityIdLengthMismatch !SegmentError
  | BlockIndexLengthMismatch !SegmentError
  | BlockExpectedEntityFields !(Cons Boxed.Vector (Field Schema.Column))
  | BlockExpectedIndexFields !(Cons Boxed.Vector (Field Schema.Column))
  | BlockExpectedAttributes !Schema.Column
  | BlockExpectedOption !Schema.Column
    deriving (Eq, Show)

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

entityHashColumn :: Boxed.Vector BlockEntity -> Striped.Column
entityHashColumn =
  Striped.Int . Storable.convert . fmap (fromIntegral . unEntityHash . entityHash)

entityIdColumn :: Boxed.Vector BlockEntity -> Striped.Column
entityIdColumn xs =
  let
    ns =
      Storable.convert $
      fmap (fromIntegral . ByteString.length . unEntityId . entityId) xs

    bytes =
      Striped.Binary .
      ByteString.concat .
      Boxed.toList $
      fmap (unEntityId . entityId) xs
  in
    Striped.Nested ns bytes

entityColumn :: Boxed.Vector BlockEntity -> Striped.Column
entityColumn xs =
  Striped.Struct $ Cons.from2
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
  (Unboxed.Vector BlockIndex -> Unboxed.Vector Int64 -> Striped.Column -> a) ->
  Boxed.Vector BlockEntity ->
  Unboxed.Vector BlockIndex ->
  Cons Boxed.Vector (Field Striped.Column) ->
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

timeColumn :: Unboxed.Vector BlockIndex -> Striped.Column
timeColumn =
  Striped.Int . Storable.convert . Unboxed.map (unTime . indexTime)

factsetColumn :: Unboxed.Vector BlockIndex -> Striped.Column
factsetColumn =
  Striped.Reversed . Striped.Int . Storable.convert . Unboxed.map (unFactsetId . indexFactsetId)

tombstoneTags :: Unboxed.Vector BlockIndex -> Storable.Vector Tag
tombstoneTags =
  Storable.map fromTombstone . Storable.convert . Unboxed.map indexTombstone

fromTombstone :: Tombstone -> Tag
fromTombstone = \case
  Tombstone ->
    0
  NotTombstone ->
    1

attributeTable :: Unboxed.Vector BlockIndex -> Unboxed.Vector Int64 -> Striped.Column -> Striped.Column
attributeTable indices0 counts values =
  let
    (key_counts, (value_counts, indices)) =
      second Unboxed.unzip $
        Generic.segmentedGroup (Unboxed.map fromIntegral counts) indices0

    key =
      Striped.Struct $ Cons.from2
        (Field "time" $ timeColumn indices)
        (Field "factset_id" $ factsetColumn indices)

    nested =
      Striped.Nested (Storable.convert $ Unboxed.map fromIntegral value_counts) .
      Striped.Array

    value =
      Striped.Enum (tombstoneTags indices0) $ Cons.from2
        (Variant "none" . Striped.Unit $ Unboxed.length indices0)
        (Variant "some" values)
  in
    Striped.Nested
      (Storable.convert $ Unboxed.map fromIntegral key_counts)
      (Striped.Map key (nested value))

blockAttributes ::
  Boxed.Vector AttributeName ->
  Block ->
  Either BlockTableError (Maybe (Cons Boxed.Vector (Field Striped.Column)))
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

  columns <- first BlockTableSchemaError $ traverse Striped.takeArray tables

  if n_names /= n_tables then
    Left $ BlockAttributeNamesDidNotMatchTableCount n_tables names
  else
    pure . Cons.fromVector $ Boxed.zipWith mkField names columns

tableOfBlock :: Boxed.Vector AttributeName -> Block -> Either BlockTableError Striped.Table
tableOfBlock names block = do
  let
    entities =
      blockEntities block

    indices =
      blockIndices block

  mfields <- blockAttributes names block

  case mfields of
    Nothing ->
      pure $ Striped.Map
        (entityColumn entities)
        (Striped.Unit $ Boxed.length entities)

    Just fields ->
      pure $ Striped.Map
        (entityColumn entities)
        (Striped.Struct $ distributeIndices attributeTable entities indices fields)

------------------------------------------------------------------------
-- Table -> Block

takeEntityHash :: Striped.Column -> Either BlockTableError (Boxed.Vector EntityHash)
takeEntityHash =
  first BlockTableSchemaError .
  fmap (fmap (EntityHash . fromIntegral) . Boxed.convert) .
  Striped.takeInt

takeEntityId :: Striped.Column -> Either BlockTableError (Boxed.Vector EntityId)
takeEntityId nested = do
  (ns, bytes0) <- first BlockTableSchemaError $ Striped.takeNested nested
  bytes <- first BlockTableSchemaError $ Striped.takeBinary bytes0

  first BlockEntityIdLengthMismatch $
    fmap EntityId <$> Segment.reify ns bytes

takeEntityKey :: Striped.Column -> Either BlockTableError (Boxed.Vector (EntityHash, EntityId))
takeEntityKey column = do
  fields <- first BlockTableSchemaError $ Striped.takeStruct column
  case Cons.toList fields of
    [Field "entity_hash" ehash, Field "entity_id" eid] ->
      Boxed.zip
        <$> takeEntityHash ehash
        <*> takeEntityId eid
    _ ->
      Left $ BlockExpectedEntityFields (fmap (fmap Striped.schemaColumn) fields)

takeAttributeRowCount :: Striped.Column -> Either BlockTableError (Unboxed.Vector Int64)
takeAttributeRowCount column = do
  (k_counts, table) <- first BlockTableSchemaError $ Striped.takeNested column
  (_k, v) <- first BlockTableSchemaError $ Striped.takeMap table
  (v_counts, _) <- first BlockTableSchemaError $ Striped.takeNested v
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

takeAttributes :: Striped.Column -> Either BlockTableError (Boxed.Vector (Field Striped.Column))
takeAttributes = \case
  Striped.Unit _ ->
    pure Boxed.empty
  Striped.Struct fields ->
    pure $ Cons.toVector fields
  x ->
    Left $ BlockExpectedAttributes (Striped.schemaColumn x)

takeAttributeRowCounts :: Striped.Column -> Either BlockTableError (Boxed.Vector (Unboxed.Vector BlockAttribute))
takeAttributeRowCounts column = do
  attrs <- takeAttributes column
  fmap (fmap fromDenseRowCounts . Generic.transpose) $
    traverse (takeAttributeRowCount . fieldData) attrs

takeEntities :: Striped.Column -> Striped.Column -> Either BlockTableError (Boxed.Vector BlockEntity)
takeEntities key0 value0 = do
  key <- takeEntityKey key0
  counts <- takeAttributeRowCounts value0
  pure $ Boxed.zipWith (uncurry BlockEntity) key counts

takeTime :: Striped.Column -> Either BlockTableError (Unboxed.Vector Time)
takeTime =
  fmap (Unboxed.map Time . Unboxed.convert) . first BlockTableSchemaError . Striped.takeInt

takeFactsetId :: Striped.Column -> Either BlockTableError (Unboxed.Vector FactsetId)
takeFactsetId column0 = do
  column <- first BlockTableSchemaError $ Striped.takeReversed column0
  fmap (Unboxed.map FactsetId . Unboxed.convert) . first BlockTableSchemaError $ Striped.takeInt column

takeIndexKey :: Striped.Column -> Either BlockTableError (Unboxed.Vector  (Time, FactsetId))
takeIndexKey column = do
  fields <- first BlockTableSchemaError $ Striped.takeStruct column
  case Cons.toList fields of
    [Field "time" time, Field "factset_id" fid] ->
      Unboxed.zip
        <$> takeTime time
        <*> takeFactsetId fid
    _ ->
      Left $ BlockExpectedIndexFields (fmap (fmap Striped.schemaColumn) fields)

fromTag :: Tag -> Tombstone
fromTag = \case
  1 ->
    NotTombstone
  _ ->
    Tombstone

takeTombstone :: Striped.Column -> Either BlockTableError (Storable.Vector Int64, Unboxed.Vector Tombstone)
takeTombstone nested = do
  (counts, array) <- first BlockTableSchemaError $ Striped.takeNested nested
  enum <- first BlockTableSchemaError $ Striped.takeArray array
  column <- fmap fst . first BlockTableSchemaError $ Striped.takeEnum enum
  pure (counts, Unboxed.convert $ Storable.map fromTag column)

replicates :: Unbox a => Storable.Vector Int64 -> Unboxed.Vector a -> Unboxed.Vector a
replicates ns xs =
  Unboxed.concatMap (uncurry Unboxed.replicate) $
  Unboxed.zip (Unboxed.map fromIntegral $ Storable.convert ns) xs

takeIndex :: Striped.Column -> Either BlockTableError (Boxed.Vector (Unboxed.Vector BlockIndex))
takeIndex column = do
  (k_counts, table) <- first BlockTableSchemaError $ Striped.takeNested column
  (k, v) <- first BlockTableSchemaError $ Striped.takeMap table

  (v_counts, tombstones) <- takeTombstone v
  ikey <- replicates v_counts <$> takeIndexKey k

  let
    indices =
      Unboxed.zipWith (uncurry BlockIndex) ikey tombstones

  kv_counts <- first BlockIndexLengthMismatch . fmap (fmap Storable.sum) $ Segment.reify k_counts v_counts

  first BlockIndexLengthMismatch $
    Segment.reify kv_counts indices

takeIndices :: Striped.Column -> Either BlockTableError (Unboxed.Vector BlockIndex)
takeIndices column = do
  attrs <- takeAttributes column
  indices <- traverse (takeIndex . fieldData) attrs
  pure .
    Unboxed.convert .
    Boxed.concatMap (Boxed.concatMap Boxed.convert) $
    Generic.transpose indices

takeTable :: Striped.Column -> Either BlockTableError Striped.Table
takeTable column0 = do
  (_ns, table0) <- first BlockTableSchemaError $ Striped.takeNested column0
  (_, value) <- first BlockTableSchemaError $ Striped.takeMap table0
  (_1s, table) <- first BlockTableSchemaError $ Striped.takeNested value
  array <- first BlockTableSchemaError $ Striped.takeArray table
  (_tag, e) <- first BlockTableSchemaError $ Striped.takeEnum array
  case Cons.toList e of
    [Variant "none" _, Variant "some" c] ->
      pure $ Striped.Array c
    _ ->
      Left $ BlockExpectedOption (Striped.schemaColumn array)

takeTables :: Striped.Column -> Either BlockTableError (Boxed.Vector Striped.Table)
takeTables column = do
  attrs <- takeAttributes column
  traverse (takeTable . fieldData) attrs

blockOfTable :: Striped.Table -> Either BlockTableError Block
blockOfTable table = do
  (k, v) <- first BlockTableSchemaError $ Striped.takeMap table
  entities <- takeEntities k v
  indices <- takeIndices v
  tables <- takeTables v
  pure $ Block entities indices tables

------------------------------------------------------------------------
-- Schema.Table -> Map AttributeName Schema.Table

fromAttribute :: AttributeName -> Schema.Column -> Field Schema.Column
fromAttribute (AttributeName name) column =
  Field (FieldName name) . Schema.Nested $
    Schema.Map
      (Schema.Struct $ Cons.from2
        (Field "time" Schema.Int)
        (Field "factset_id" $ Schema.Reversed Schema.Int))
      (Schema.Nested . Schema.Array $ Schema.option column)

fromFields :: Boxed.Vector (Field Schema.Column) -> Schema.Column
fromFields xs0 =
  case Cons.fromVector xs0 of
    Nothing ->
      Schema.Unit
    Just xs ->
      Schema.Struct xs

tableSchemaOfAttributes :: Map AttributeName Schema.Column -> Schema.Table
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
-- Map AttributeName Schema.Table -> Schema.Table

takeAttribute :: Field Schema.Column -> Either BlockTableError (AttributeName, Schema.Column)
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

takeFields :: Schema.Column -> Either BlockTableError (Boxed.Vector (Field Schema.Column))
takeFields = \case
  Schema.Unit ->
    pure Boxed.empty
  Schema.Struct fs ->
    pure $ Cons.toVector fs
  x ->
    Left $ BlockExpectedAttributes x

attributesOfTableSchema :: Schema.Table -> Either BlockTableError (Map AttributeName Schema.Column)
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
