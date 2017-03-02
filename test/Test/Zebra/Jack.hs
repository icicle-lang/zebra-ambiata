{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Data.Block
    jBlock

  , jYoloBlock
  , jBlockEntity
  , jBlockAttribute
  , jBlockIndex
  , jTombstone

  -- * Zebra.Data.Core
  , jEntityId
  , jEntityHashId
  , jAttributeId
  , jAttributeName
  , jTime
  , jDay
  , jFactsetId

  -- * Zebra.Data.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Data.Fact
  , jFacts
  , jFact

  -- * Zebra.Data.Encoding
  , jEncoding
  , jColumnEncoding

  -- * Zebra.Schema
  , jSchema
  , jField
  , jFieldName
  , jVariant
  , jVariantName

  -- * Zebra.Value
  , jValue

  -- * Zebra.Table
  , jAnyTable
  , jTable

  , jMaybe'
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.List as List
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed
import           Data.Word (Word8)

import           Disorder.Corpus (muppets, southpark, boats, weather)
import           Disorder.Jack (Jack, mkJack, reshrink, shrinkTowards, sized, scale)
import           Disorder.Jack (elements, arbitrary, choose, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOf, listOfN, vectorOf, justOf, maybeOf)

import           P

import qualified Prelude as Savage

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Schema (Schema, Variant(..), VariantName(..), Field(..), FieldName(..))
import qualified Zebra.Schema as Schema
import           Zebra.Table (Table(..), Column(..))
import qualified Zebra.Table as Table
import           Zebra.Value (Value)
import qualified Zebra.Value as Value


jEncoding :: Jack Encoding
jEncoding =
  Encoding <$> listOf jColumnEncoding

jColumnEncoding :: Jack ColumnEncoding
jColumnEncoding =
  oneOfRec [
      pure IntEncoding
    , pure ByteEncoding
    , pure DoubleEncoding
    ] [
      ArrayEncoding <$> jEncoding
    ]

schemaSubterms :: Schema -> [Schema]
schemaSubterms = \case
  Schema.Byte ->
    []
  Schema.Int ->
    []
  Schema.Double ->
    []
  Schema.Enum variant0 variants ->
    fmap Schema.variantSchema $ variant0 : Boxed.toList variants
  Schema.Struct ss ->
    fmap fieldSchema $ Boxed.toList ss
  Schema.Array item ->
    [item]

jSchema :: Jack Schema
jSchema =
  reshrink schemaSubterms $
  oneOfRec [
      pure Schema.Byte
    , pure Schema.Int
    , pure Schema.Double
    ] [
      Schema.Enum <$> jVariant <*> (Boxed.fromList <$> smallListOf jVariant)
    , Schema.Struct . Boxed.fromList <$> smallListOf jField
    , Schema.Array <$> jSchema
    ]

smallListOf :: Jack a -> Jack [a]
smallListOf gen =
  sized $ \n ->
    listOfN 0 (n `div` 10) gen

jField :: Jack Field
jField =
  Field <$> jFieldName <*> jSchema

jFieldName :: Jack FieldName
jFieldName =
  FieldName <$> elements boats

jVariant :: Jack Variant
jVariant =
  Variant <$> jVariantName <*> jSchema

jVariantName :: Jack VariantName
jVariantName =
  VariantName <$> elements weather

jFacts :: [Schema] -> Jack [Fact]
jFacts schemas =
  fmap (List.sort . List.concat) .
  scale (`div` max 1 (length schemas)) $
  zipWithM (\e a -> listOf $ jFact e a) schemas (fmap AttributeId [0..])

jFact :: Schema -> AttributeId -> Jack Fact
jFact schema aid =
  uncurry Fact
    <$> jEntityHashId
    <*> pure aid
    <*> jTime
    <*> jFactsetId
    <*> (strictMaybe <$> maybeOf (jValue schema))

jValue :: Schema -> Jack Value
jValue = \case
  Schema.Byte ->
    Value.Byte <$> sizedBounded
  Schema.Int ->
    Value.Int <$> sizedBounded
  Schema.Double ->
    Value.Double <$> arbitrary
  Schema.Enum variant0 variants -> do
    tag <- choose (0, Boxed.length variants)
    case Schema.lookupVariant tag variant0 variants of
      Nothing ->
        Savage.error $ renderTagLookupError tag variant0 variants
      Just (Variant _ schema) ->
        Value.Enum tag <$> jValue schema
  Schema.Struct fields ->
    Value.Struct <$> traverse (jValue . fieldSchema) fields
  Schema.Array Schema.Byte ->
    Value.ByteArray <$> arbitrary
  Schema.Array schema ->
    Value.Array . Boxed.fromList <$> listOfN 0 10 (jValue schema)

renderTagLookupError :: Int -> Variant -> Boxed.Vector Variant -> [Char]
renderTagLookupError tag variant0 variants =
  "jValue: internal error, tag not found" <>
  "\n" <>
  "\n  tag = " <> show tag <>
  "\n" <>
  "\n  variants =" <>
  (List.concatMap ("\n    " <>) . List.lines . ppShow $ Boxed.cons variant0 variants) <>
  "\n"

jEntityId :: Jack EntityId
jEntityId =
  let
    mkEnt name num =
      EntityId $ name <> Char8.pack (printf "-%03d" num)
  in
    mkEnt
      <$> elements southpark
      <*> chooseInt (0, 999)

jAttributeId :: Jack AttributeId
jAttributeId =
  AttributeId <$> choose (0, 10000)

jAttributeName :: Jack AttributeName
jAttributeName =
  AttributeName <$> oneOf [elements muppets, arbitrary]

jTime :: Jack Time
jTime =
  fromDay <$> jDay

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
  FactsetId <$> choose (0, 100000)

jBlock :: Jack (Block Schema)
jBlock = do
  schemas <- listOfN 0 5 jSchema
  facts <- jFacts schemas
  pure $
    case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
      Left x ->
        Savage.error $ "Test.Zebra.Jack.jBlock: invariant failed: " <> show x
      Right x ->
        x

-- The blocks generated by this can contain data with broken invariants.
jYoloBlock :: Jack (Block Schema)
jYoloBlock = do
  sized $ \size ->
    Block
      <$> (Boxed.fromList <$> listOfN 0 (size `div` 5) jBlockEntity)
      <*> (Unboxed.fromList <$> listOfN 0 (size `div` 5) jBlockIndex)
      <*> (Boxed.fromList <$> listOfN 0 (size `div` 5) jAnyTable)

jEntityHashId :: Jack (EntityHash, EntityId)
jEntityHashId =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

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

jEntity :: Jack (Entity Schema)
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Boxed.fromList <$> listOf jAttribute)

jAttribute :: Jack (Attribute Schema)
jAttribute = do
  (ts, ps, bs) <- List.unzip3 <$> listOf ((,,) <$> jTime <*> jFactsetId <*> jTombstone)
  schema <- jSchema
  Attribute
    <$> pure (Storable.fromList ts)
    <*> pure (Storable.fromList ps)
    <*> pure (Storable.fromList bs)
    <*> jTable (List.length ts) schema

jTombstone :: Jack Tombstone
jTombstone =
  elements [
      NotTombstone
    , Tombstone
    ]

jAnyTable :: Jack (Table Schema)
jAnyTable =
  sized $ \size -> do
    n <- chooseInt (0, size `div` 5)
    schema <- jSchema
    jTable n schema

jTable :: Int -> Schema -> Jack (Table Schema)
jTable n = \case
  Schema.Byte ->
    jByteTable n
  Schema.Int ->
    jIntTable n
  Schema.Double ->
    jDoubleTable n
  Schema.Enum variant0 variants ->
    jEnumTable n variant0 variants
  Schema.Struct fields ->
    jStructTable n fields
  Schema.Array item ->
    jArrayTable n item

jByteTable :: Int -> Jack (Table Schema)
jByteTable n =
  Table Schema.Byte n . mkByteColumn <$>
    vectorOf n arbitrary

jIntTable :: Int -> Jack (Table Schema)
jIntTable n =
  Table Schema.Int n . mkIntColumn <$>
    vectorOf n arbitrary

jDoubleTable :: Int -> Jack (Table Schema)
jDoubleTable n =
  Table Schema.Double n . mkDoubleColumn <$>
    vectorOf n arbitrary

jEnumTable :: Int -> Variant -> Boxed.Vector Variant -> Jack (Table Schema)
jEnumTable n variant0 variants =
  Table (Schema.Enum variant0 variants) n <$> do
    tags <- mkIntColumn <$> replicateM n (choose (0, fromIntegral $ Boxed.length variants))
    v <- Table.columns <$> jTable n (Schema.variantSchema variant0)
    vs <- Boxed.concatMap Table.columns <$> traverse (jTable n . Schema.variantSchema) variants
    pure $ tags <> v <> vs

jStructTable :: Int -> Boxed.Vector Field -> Jack (Table Schema)
jStructTable n fields =
  Table (Schema.Struct fields) n <$>
    Boxed.concatMap Table.columns <$> traverse (jTable n . Schema.fieldSchema) fields

jArrayTable :: Int -> Schema -> Jack (Table Schema)
jArrayTable n schema =
  fmap (Table (Schema.Array schema) n) . sized $ \size -> do
    lengths <- vectorOf n $ chooseInt (0, size `div` 10)
    mkArrayColumn lengths <$> jTable (sum lengths) schema

mkByteColumn :: [Word8] -> Boxed.Vector (Column Schema)
mkByteColumn =
  Boxed.singleton . ByteColumn . B.pack

mkIntColumn :: [Int64] -> Boxed.Vector (Column Schema)
mkIntColumn =
  Boxed.singleton . IntColumn . Storable.fromList

mkDoubleColumn :: [Double] -> Boxed.Vector (Column Schema)
mkDoubleColumn =
  Boxed.singleton . DoubleColumn . Storable.fromList

mkArrayColumn :: [Int] -> Table Schema -> Boxed.Vector (Column Schema)
mkArrayColumn lengths =
  Boxed.singleton . ArrayColumn (Storable.fromList $ fmap fromIntegral lengths)

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]
