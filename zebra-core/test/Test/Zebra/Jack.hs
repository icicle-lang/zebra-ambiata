{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- -- * Zebra.Data.Block
    jBlock
  , jYoloBlock
  , jBlockEntity
  , jBlockAttribute
  , jBlockIndex
  , jTombstone

  -- * Zebra.Data.Core
  , jBinaryVersion
  , jEntityId
  , jEntityHashId
  , jAttributeId
  , jAttributeName
  , jTime
  , jDay
  , jFactsetId

  -- -- * Zebra.Data.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Data.Fact
  , jFacts
  , jFact

  -- * Zebra.Schema
  , jField
  , jFieldName
  , jVariant
  , jVariantName

  -- * Zebra.Schema
  , jTableSchema
  , jMapSchema
  , jColumnSchema

  -- * Zebra.Table
  , jSizedTable
  , jTable
  , jArrayTable
  , jColumn

  -- * Zebra.Value
  , jSizedCollection
  , jCollection
  , jValue

  , jMaybe'

  -- * Normalization
  , normalizeTable
  , normalizeCollection
  , normalizeValue
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Char as Char
import qualified Data.List as List
import qualified Data.Map as Map
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Corpus (muppets, southpark, boats, weather)
import           Disorder.Jack (Jack, mkJack, reshrink, shrinkTowards, sized, scale)
import           Disorder.Jack (elements, arbitrary, choose, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOf, listOfN, vectorOf, justOf, maybeOf)
import           Disorder.Jack (boundedEnum)

import           P

import qualified Prelude as Savage

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Fact

import           Zebra.Binary.Data
import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Schema (TableSchema, ColumnSchema)
import           Zebra.Schema (Variant(..), VariantName(..), Field(..), FieldName(..), Tag)
import qualified Zebra.Schema as Schema
import           Zebra.Table (Table, Column)
import qualified Zebra.Table as Table
import           Zebra.Value (Collection, Value)
import qualified Zebra.Value as Value

------------------------------------------------------------------------

jField :: Jack a -> Jack (Field a)
jField gen =
  Field <$> jFieldName <*> gen

jFieldName :: Jack FieldName
jFieldName =
  FieldName <$> elements boats

jVariant :: Jack a -> Jack (Variant a)
jVariant gen =
  Variant <$> jVariantName <*> gen

jVariantName :: Jack VariantName
jVariantName =
  VariantName <$> elements weather

------------------------------------------------------------------------

tableSchemaTables :: TableSchema -> [TableSchema]
tableSchemaTables = \case
  Schema.Binary ->
    []
  Schema.Array x ->
    columnSchemaTables x
  Schema.Map k v ->
    columnSchemaTables k <>
    columnSchemaTables v

tableSchemaColumns :: TableSchema -> [ColumnSchema]
tableSchemaColumns = \case
  Schema.Binary ->
    []
  Schema.Array x ->
    [x]
  Schema.Map k v ->
    [k, v]

columnSchemaTables :: ColumnSchema -> [TableSchema]
columnSchemaTables = \case
  Schema.Unit ->
    []
  Schema.Int ->
    []
  Schema.Double ->
    []
  Schema.Enum variants ->
    concatMap columnSchemaTables . fmap Schema.variant $
      Cons.toList variants
  Schema.Struct fields ->
    concatMap columnSchemaTables . fmap Schema.field $
      Cons.toList fields
  Schema.Nested table ->
    [table]
  Schema.Reversed schema ->
    columnSchemaTables schema

columnSchemaColumns :: ColumnSchema -> [ColumnSchema]
columnSchemaColumns = \case
  Schema.Unit ->
    []
  Schema.Int ->
    []
  Schema.Double ->
    []
  Schema.Enum variants ->
    fmap Schema.variant $ Cons.toList variants
  Schema.Struct fields ->
    fmap Schema.field $ Cons.toList fields
  Schema.Nested table ->
    tableSchemaColumns table
  Schema.Reversed schema ->
    [schema]

jTableSchema :: Jack TableSchema
jTableSchema =
  reshrink tableSchemaTables $
  oneOfRec [
      pure Schema.Binary
    ] [
      Schema.Array <$> jColumnSchema
    , jMapSchema
    ]

jMapSchema :: Jack TableSchema
jMapSchema =
  Schema.Map <$> jColumnSchema <*> jColumnSchema

jColumnSchema :: Jack ColumnSchema
jColumnSchema =
  reshrink columnSchemaColumns $
  oneOfRec [
      pure Schema.Int
    , pure Schema.Double
    ] [
      Schema.Enum <$> smallConsOf (jVariant jColumnSchema)
    , Schema.Struct <$> smallConsOf (jField jColumnSchema)
    , Schema.Nested <$> jTableSchema
    , Schema.Reversed <$> jColumnSchema
    ]

------------------------------------------------------------------------

tableTables :: Table -> [Table]
tableTables = \case
  Table.Binary _ ->
    []
  Table.Array x ->
    columnTables x
  Table.Map k v ->
    columnTables k <>
    columnTables v

tableColumns :: Table -> [Column]
tableColumns = \case
  Table.Binary _ ->
    []
  Table.Array x ->
    [x]
  Table.Map k v ->
    [k, v]

columnTables :: Column -> [Table]
columnTables = \case
  Table.Unit _ ->
    []
  Table.Int _ ->
    []
  Table.Double _ ->
    []
  Table.Enum _ variants ->
    concatMap columnTables $
      fmap Schema.variant (Cons.toList variants)
  Table.Struct fields ->
    concatMap columnTables $
      fmap Schema.field (Cons.toList fields)
  Table.Nested _ table ->
    [table]
  Table.Reversed column ->
    columnTables column

columnColumns :: Column -> [Column]
columnColumns = \case
  Table.Unit _ ->
    []
  Table.Int _ ->
    []
  Table.Double _ ->
    []
  Table.Enum _ variants ->
    fmap Schema.variant $ Cons.toList variants
  Table.Struct fields ->
    fmap Schema.field $ Cons.toList fields
  Table.Nested _ table ->
    tableColumns table
  Table.Reversed column ->
    columnColumns column

jSizedTable :: Jack Table
jSizedTable =
  sized $ \size ->
    jTable =<< chooseInt (0, size `div` 5)

jTable :: Int -> Jack Table
jTable n =
  reshrink tableTables $
  oneOfRec [
      jBinaryTable n
    ] [
      jArrayTable n
    , jMapTable n
    ]

jByteString :: Int -> Jack ByteString
jByteString n =
  oneOf [
      Char8.pack <$> vectorOf n (fmap Char.chr $ chooseInt (Char.ord 'a', Char.ord 'z'))
    , ByteString.pack <$> vectorOf n boundedEnum
    ]

jBinaryTable  :: Int -> Jack Table
jBinaryTable n =
  Table.Binary <$> jByteString n

jArrayTable :: Int -> Jack Table
jArrayTable n = do
  Table.Array
    <$> jColumn n

jMapTable :: Int -> Jack Table
jMapTable n = do
  Table.Map
    <$> jColumn n
    <*> jColumn n

jColumn :: Int -> Jack Column
jColumn n =
  reshrink columnColumns $
  oneOfRec [
      jIntColumn n
    , jDoubleColumn n
    ] [
      jEnumColumn n
    , jStructColumn n
    , jNestedColumn n
    , jReversedColumn n
    ]

jIntColumn :: Int -> Jack Column
jIntColumn n =
  Table.Int . Storable.fromList <$> vectorOf n sizedBounded

jDoubleColumn :: Int -> Jack Column
jDoubleColumn n =
  Table.Double . Storable.fromList <$> vectorOf n arbitrary

jEnumColumn :: Int -> Jack Column
jEnumColumn n = do
  sized $ \size -> do
    ntags <- chooseInt (1, 1 + (size `div` 10))
    tags <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, ntags - 1))
    vs <- Cons.unsafeFromList <$> vectorOf ntags (jVariant . jColumn $ Storable.length tags)
    pure $
      Table.Enum tags vs

jStructColumn :: Int -> Jack Column
jStructColumn n =
  Table.Struct <$> smallConsOf (jField (jColumn n))

jNestedColumn :: Int -> Jack Column
jNestedColumn n =
  sized $ \size -> do
    ns <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, size `div` 10))
    Table.Nested ns <$> jTable (fromIntegral $ Storable.sum ns)

jReversedColumn :: Int -> Jack Column
jReversedColumn n =
  Table.Reversed <$> jColumn n

------------------------------------------------------------------------

jFacts :: [ColumnSchema] -> Jack [Fact]
jFacts schemas =
  fmap (List.sort . List.concat) .
  scale (`div` max 1 (length schemas)) $
  zipWithM (\e a -> listOf $ jFact e a) schemas (fmap AttributeId [0..])

jFact :: ColumnSchema -> AttributeId -> Jack Fact
jFact schema aid =
  uncurry Fact
    <$> jEntityHashId
    <*> pure aid
    <*> jTime
    <*> jFactsetId
    <*> (strictMaybe <$> maybeOf (jValue schema))

jSizedCollection :: TableSchema -> Jack Collection
jSizedCollection schema =
  sized $ \size ->
    jCollection schema =<< chooseInt (0, size `div` 5)

jCollection :: TableSchema -> Int -> Jack Collection
jCollection tschema n =
  case tschema of
    Schema.Binary ->
      Value.Binary <$> jByteString n
    Schema.Array x ->
      Value.Array . Boxed.fromList <$> vectorOf n (jValue x)
    Schema.Map k v ->
      Value.Map . Map.fromList <$> vectorOf n (jMapping k v)

jMapping :: ColumnSchema -> ColumnSchema -> Jack (Value, Value)
jMapping k v =
  (,) <$> jValue k <*> jValue v

jTag :: Cons Boxed.Vector (Variant a) -> Jack Tag
jTag xs =
  fromIntegral <$> choose (0, Cons.length xs - 1)

jValue :: ColumnSchema -> Jack Value
jValue = \case
  Schema.Unit ->
    pure Value.Unit

  Schema.Int ->
    Value.Int <$> sizedBounded

  Schema.Double ->
    Value.Double <$> arbitrary

  Schema.Enum variants -> do
    tag <- jTag variants
    case Schema.lookupVariant tag variants of
      Nothing ->
        Savage.error $ renderTagLookupError tag variants
      Just (Variant _ schema) ->
        Value.Enum tag <$> jValue schema

  Schema.Struct fields ->
    Value.Struct <$> traverse (jValue . Schema.field) fields

  Schema.Nested tschema ->
    sized $ \size -> do
      fmap Value.Nested $ jCollection tschema =<< chooseInt (0, size `div` 10)

  Schema.Reversed schema ->
    Value.Reversed <$> jValue schema

renderTagLookupError :: Show a => Tag -> Cons Boxed.Vector (Variant a) -> [Char]
renderTagLookupError tag variants =
  "jValue: internal error, tag not found" <>
  "\n" <>
  "\n  tag = " <> show tag <>
  "\n" <>
  "\n  variants =" <>
  (List.concatMap ("\n    " <>) . List.lines $ ppShow variants) <>
  "\n"

jBinaryVersion :: Jack BinaryVersion
jBinaryVersion =
  elements [BinaryV2, BinaryV3]

jEntityId :: Jack EntityId
jEntityId =
  let
    mkEnt :: ByteString -> Int -> EntityId
    mkEnt name num =
      EntityId $ name <> Char8.pack (printf "-%03d" num)
  in
    oneOf [
        mkEnt <$> elements southpark <*> pure 0
      , mkEnt <$> elements southpark <*> chooseInt (0, 999)
      ]

jEntityHashId :: Jack (EntityHash, EntityId)
jEntityHashId =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

jAttributeId :: Jack AttributeId
jAttributeId =
  AttributeId <$> choose (0, 10000)

jAttributeName :: Jack AttributeName
jAttributeName =
  AttributeName <$> oneOf [elements muppets, arbitrary]

jTime :: Jack Time
jTime =
  oneOf [
      Time <$> choose (0, 5)
    , fromDay <$> jDay
    ]

jDay :: Jack Day
jDay =
  justOf . fmap gregorianValid $
    YearMonthDay
      <$> jYear
      <*> chooseInt (1, 12)
      <*> chooseInt (1, 31)

jYear :: Jack Year
jYear =
  mkJack (shrinkTowards 2000) $ QC.choose (1600, 3000)

jFactsetId :: Jack FactsetId
jFactsetId =
  oneOf [
      FactsetId <$> choose (0, 5)
    , FactsetId <$> choose (0, 100000)
    ]

jBlock :: Jack Block
jBlock = do
  schemas <- listOfN 0 5 jColumnSchema
  facts <- jFacts schemas
  pure $
    case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
      Left x ->
        Savage.error $ "Test.Zebra.Jack.jBlock: invariant failed: " <> show x
      Right x ->
        x

-- The blocks generated by this can contain data with broken invariants.
jYoloBlock :: Jack Block
jYoloBlock = do
  sized $ \size ->
    Block
      <$> (Boxed.fromList <$> listOfN 0 (size `div` 5) jBlockEntity)
      <*> (Unboxed.fromList <$> listOfN 0 (size `div` 5) jBlockIndex)
      <*> (Boxed.fromList <$> listOfN 0 (size `div` 5) (jArrayTable =<< chooseInt (0, size `div` 5)))

jBlockEntity :: Jack BlockEntity
jBlockEntity =
  uncurry BlockEntity
    <$> jEntityHashId
    <*> (Unboxed.fromList <$> listOf jBlockAttribute)

jBlockAttribute :: Jack BlockAttribute
jBlockAttribute =
  BlockAttribute
    <$> jAttributeId
    <*> choose (0, 1000000)

jBlockIndex :: Jack BlockIndex
jBlockIndex =
  BlockIndex
    <$> jTime
    <*> jFactsetId
    <*> jTombstone

jEntity :: Jack Entity
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Boxed.fromList <$> listOf jAttribute)

jAttribute :: Jack Attribute
jAttribute = do
  (ts, ps, bs) <- List.unzip3 <$> listOf ((,,) <$> jTime <*> jFactsetId <*> jTombstone)
  Attribute
    <$> pure (Storable.fromList ts)
    <*> pure (Storable.fromList ps)
    <*> pure (Storable.fromList bs)
    <*> jArrayTable (List.length ts)

jTombstone :: Jack Tombstone
jTombstone =
  elements [
      NotTombstone
    , Tombstone
    ]

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]

smallConsOf :: Jack a -> Jack (Cons Boxed.Vector a)
smallConsOf gen =
  sized $ \n ->
    Cons.unsafeFromList <$> listOfN 1 (1 + (n `div` 10)) gen

------------------------------------------------------------------------

normalizeTable :: Table -> Table
normalizeTable table =
  let
    Right x =
      Table.fromCollection (Table.schema table) . normalizeCollection =<<
      Table.toCollection table
  in
    x

normalizeCollection :: Collection -> Collection
normalizeCollection = \case
  Value.Binary bs ->
    Value.Binary $ ByteString.sort bs
  Value.Array xs ->
    Value.Array . Boxed.fromList . List.sort $ Boxed.toList xs
  Value.Map kvs ->
    Value.Map $ fmap normalizeValue kvs

normalizeValue :: Value -> Value
normalizeValue = \case
  Value.Unit ->
    Value.Unit
  Value.Int x ->
    Value.Int x
  Value.Double x ->
    Value.Double x
  Value.Enum tag x ->
    Value.Enum tag (normalizeValue x)
  Value.Struct xs ->
    Value.Struct $ fmap normalizeValue xs
  Value.Nested x ->
    Value.Nested $ normalizeCollection x
  Value.Reversed x ->
    Value.Reversed $ normalizeValue x
