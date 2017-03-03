{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table (
    Table(..)
  , Column(..)

  , TableError(..)
  , renderTableError

  , TableSchemaError(..)
  , renderTableSchemaError

  , length

  , schema
  , schemaColumn

  , empty
  , emptyColumn

  , takeBinary
  , takeArray
  , takeMap
  , takeInt
  , takeDouble
  , takeEnum
  , takeStruct
  , takeNested
  , takeReversed

  , fromCollection
  , fromValues

  , toCollection
  , toValues

  , splitAt
  , splitAtColumn

  , unions
  , union

  , unsafeConcat
  , unsafeAppend
  , unsafeAppendColumn
  ) where

import           Data.Biapplicative (biliftA2)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Internal (ByteString(..))
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           P hiding (empty, concat, splitAt, length)

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Generic as Generic

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import qualified Zebra.Data.Vector.Storable as Storable
import           Zebra.Schema (TableSchema, ColumnSchema, Field, Variant, Tag)
import qualified Zebra.Schema as Schema
import           Zebra.Segment (SegmentError)
import qualified Zebra.Segment as Segment
import           Zebra.Value (Collection, Value, ValueSchemaError, ValueUnionError)
import qualified Zebra.Value as Value


data Table =
    Binary !ByteString
  | Array !Column
  | Map !Column !Column
    deriving (Eq, Ord, Show, Generic, Typeable)

data Column =
    Unit !Int
  | Int !(Storable.Vector Int64)
  | Double !(Storable.Vector Double)
  | Enum !(Storable.Vector Tag) !(Cons Boxed.Vector (Variant Column))
  | Struct !(Cons Boxed.Vector (Field Column))
  | Nested !(Storable.Vector Int64) !Table
  | Reversed !Column
    deriving (Eq, Ord, Show, Generic, Typeable)

data TableError =
    TableValueSchemaError !ValueSchemaError
  | TableValueUnionError !ValueUnionError
  | TableNoValueForEnumTag !Tag !(Cons Boxed.Vector Value)
  | TableNestedLengthMismatch !TableSchema !SegmentError
  | TableAppendTableMismatch !TableSchema !TableSchema
  | TableAppendColumnMismatch !ColumnSchema !ColumnSchema
  | TableAppendVariantMismatch !(Variant ColumnSchema) !(Variant ColumnSchema)
  | TableAppendFieldMismatch !(Field ColumnSchema) !(Field ColumnSchema)
    deriving (Eq, Ord, Show, Generic, Typeable)

data TableSchemaError =
    TableExpectedBinary !TableSchema
  | TableExpectedArray !TableSchema
  | TableExpectedMap !TableSchema
  | TableExpectedInt !ColumnSchema
  | TableExpectedDouble !ColumnSchema
  | TableExpectedEnum !ColumnSchema
  | TableExpectedStruct !ColumnSchema
  | TableExpectedNested !ColumnSchema
  | TableExpectedReversed !ColumnSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

renderTableError :: TableError -> Text
renderTableError = \case
  TableValueSchemaError err ->
    Value.renderValueSchemaError err

  TableValueUnionError err ->
    Value.renderValueUnionError err

  TableNoValueForEnumTag tag values ->
    "Cannot construct enum with <" <>
    Text.pack (show tag) <>
    "> from values: " <>
    Text.pack (ppShow values)

  TableNestedLengthMismatch ts err ->
    "Failed to split table: " <> Text.pack (ppShow ts) <>
    "\n" <> Segment.renderSegmentError err

  TableAppendTableMismatch x y ->
    "Cannot append tables with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  TableAppendColumnMismatch x y ->
    "Cannot append columns with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  TableAppendVariantMismatch x y ->
    "Cannot append variants with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  TableAppendFieldMismatch x y ->
    "Cannot append fields with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

renderTableSchemaError :: TableSchemaError -> Text
renderTableSchemaError = \case
  TableExpectedBinary x ->
    "Expected binary, but was: " <> Text.pack (ppShow x)
  TableExpectedArray x ->
    "Expected array, but was: " <> Text.pack (ppShow x)
  TableExpectedMap x ->
    "Expected map, but was: " <> Text.pack (ppShow x)
  TableExpectedInt x ->
    "Expected int, but was: " <> Text.pack (ppShow x)
  TableExpectedDouble x ->
    "Expected double, but was: " <> Text.pack (ppShow x)
  TableExpectedEnum x ->
    "Expected enum, but was: " <> Text.pack (ppShow x)
  TableExpectedStruct x ->
    "Expected struct, but was: " <> Text.pack (ppShow x)
  TableExpectedNested x ->
    "Expected nested, but was: " <> Text.pack (ppShow x)
  TableExpectedReversed x ->
    "Expected reversed, but was: " <> Text.pack (ppShow x)

ppField :: Show a => Text -> a -> Text
ppField name x =
  "\n" <>
  "\n  " <> name <> " =" <>
  ppPrefix "\n    " x

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix =
  Text.concat . fmap (prefix <>) . Text.lines . Text.pack . ppShow

------------------------------------------------------------------------

length :: Table -> Int
length = \case
  Binary bs ->
    ByteString.length bs
  Array c ->
    lengthColumn c
  Map k v ->
    -- FIXME This is doing more work than required, would
    -- FIXME be good to have a 'validate :: Table -> Bool'
    -- FIXME function that checks a table has matching
    -- FIXME lengths instead perhaps?
    min (lengthColumn k) (lengthColumn v)

lengthColumn :: Column -> Int
lengthColumn = \case
  Unit n ->
    n
  Int xs ->
    Storable.length xs
  Double xs ->
    Storable.length xs
  Enum tags _ ->
    Storable.length tags
  Struct fs ->
    lengthColumn . Schema.field $ Cons.head fs
  Nested ns _ ->
    Storable.length ns
  Reversed c ->
    lengthColumn c

schema :: Table -> TableSchema
schema = \case
  Binary _ ->
    Schema.Binary
  Array x ->
    Schema.Array (schemaColumn x)
  Map k v ->
    Schema.Map (schemaColumn k) (schemaColumn v)

schemaColumn :: Column -> ColumnSchema
schemaColumn = \case
  Unit _ ->
    Schema.Unit
  Int _ ->
    Schema.Int
  Double _ ->
    Schema.Double
  Enum _ vs ->
    Schema.Enum (fmap (fmap schemaColumn) vs)
  Struct fs ->
    Schema.Struct (fmap (fmap schemaColumn) fs)
  Nested _ t ->
    Schema.Nested (schema t)
  Reversed c ->
    Schema.Reversed (schemaColumn c)

------------------------------------------------------------------------

empty :: TableSchema -> Table
empty = \case
  Schema.Binary ->
    Binary ByteString.empty
  Schema.Array x ->
    Array (emptyColumn x)
  Schema.Map k v ->
    Map (emptyColumn k) (emptyColumn v)

emptyColumn :: ColumnSchema -> Column
emptyColumn = \case
  Schema.Unit ->
    Unit 0
  Schema.Int ->
    Int Storable.empty
  Schema.Double ->
    Double Storable.empty
  Schema.Enum vs ->
    Enum Storable.empty (fmap (fmap emptyColumn) vs)
  Schema.Struct fs ->
    Struct (fmap (fmap emptyColumn) fs)
  Schema.Nested t ->
    Nested Storable.empty (empty t)
  Schema.Reversed c ->
    Reversed (emptyColumn c)

------------------------------------------------------------------------

takeBinary :: Table -> Either TableSchemaError ByteString
takeBinary = \case
  Binary x ->
    Right x
  x ->
    Left $ TableExpectedBinary (schema x)
{-# INLINE takeBinary #-}

takeArray :: Table -> Either TableSchemaError Column
takeArray = \case
  Array x ->
    Right x
  x ->
    Left $ TableExpectedArray (schema x)
{-# INLINE takeArray #-}

takeMap :: Table -> Either TableSchemaError (Column, Column)
takeMap = \case
  Map k v ->
    Right (k, v)
  x ->
    Left $ TableExpectedMap (schema x)
{-# INLINE takeMap #-}

takeInt :: Column -> Either TableSchemaError (Storable.Vector Int64)
takeInt = \case
  Int x ->
    Right x
  x ->
    Left $ TableExpectedInt (schemaColumn x)
{-# INLINE takeInt #-}

takeDouble :: Column -> Either TableSchemaError (Storable.Vector Double)
takeDouble = \case
  Double x ->
    Right x
  x ->
    Left $ TableExpectedDouble (schemaColumn x)
{-# INLINE takeDouble #-}

takeEnum :: Column -> Either TableSchemaError (Storable.Vector Tag, Cons Boxed.Vector (Variant Column))
takeEnum = \case
  Enum tags x ->
    Right (tags, x)
  x ->
    Left $ TableExpectedEnum (schemaColumn x)
{-# INLINE takeEnum #-}

takeStruct :: Column -> Either TableSchemaError (Cons Boxed.Vector (Field Column))
takeStruct = \case
  Struct x ->
    Right x
  x ->
    Left $ TableExpectedStruct (schemaColumn x)
{-# INLINE takeStruct #-}

takeNested :: Column -> Either TableSchemaError (Storable.Vector Int64, Table)
takeNested = \case
  Nested ns x ->
    Right (ns, x)
  x ->
    Left $ TableExpectedNested (schemaColumn x)
{-# INLINE takeNested #-}

takeReversed :: Column -> Either TableSchemaError Column
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ TableExpectedReversed (schemaColumn x)
{-# INLINE takeReversed #-}

------------------------------------------------------------------------
-- Collection/Value -> Table/Column

fromCollection :: TableSchema -> Collection -> Either TableError Table
fromCollection tschema collection =
  case tschema of
    Schema.Binary ->
      Binary
        <$> first TableValueSchemaError (Value.takeBinary collection)

    Schema.Array eschema -> do
      xs <- first TableValueSchemaError $ Value.takeArray collection
      Array
        <$> fromValues eschema xs

    Schema.Map kschema vschema -> do
      kvs <- first TableValueSchemaError $ Value.takeMap collection
      Map
        <$> fromValues kschema (Boxed.fromList $ Map.keys kvs)
        <*> fromValues vschema (Boxed.fromList $ Map.elems kvs)

fromNested :: TableSchema -> Boxed.Vector Collection -> Either TableError (Storable.Vector Int64, Table)
fromNested tschema xss0 =
  case tschema of
    Schema.Binary -> do
      bss <- first TableValueSchemaError $ traverse Value.takeBinary xss0
      pure (
          Storable.convert $ fmap (fromIntegral . ByteString.length) bss
        , Binary . ByteString.concat $ Boxed.toList bss
        )

    Schema.Array eschema -> do
      xss <- first TableValueSchemaError $ traverse Value.takeArray xss0
      column <- fromValues eschema . Boxed.concat $ Boxed.toList xss
      pure (
          Storable.convert $ fmap (fromIntegral . Boxed.length) xss
        , Array column
        )

    Schema.Map kschema vschema -> do
      kvss <- first TableValueSchemaError $ traverse Value.takeMap xss0

      let
        (ks0, vs0) =
          Boxed.unzip $ Boxed.concatMap (Boxed.fromList . Map.toList) kvss

      ks <- fromValues kschema ks0
      vs <- fromValues vschema vs0

      pure (
          Storable.convert $ fmap (fromIntegral . Map.size) kvss
        , Map ks vs
        )

fromValues :: ColumnSchema -> Boxed.Vector Value -> Either TableError Column
fromValues cschema values =
  case Cons.fromVector values of
    Nothing ->
      pure $ emptyColumn cschema
    Just values1 ->
      case cschema of
        Schema.Unit ->
          pure . Unit $ Boxed.length values

        Schema.Int ->
          Int . Storable.convert
            <$> traverse (first TableValueSchemaError . Value.takeInt) values

        Schema.Double ->
          Double . Storable.convert
            <$> traverse (first TableValueSchemaError . Value.takeDouble) values

        Schema.Enum vs -> do
          txs <- traverse (first TableValueSchemaError . Value.takeEnum) values

          let
            tags =
              Storable.convert $ fmap (fromIntegral . fst) txs

          Enum
            <$> pure tags
            <*> fromEnum vs txs

        Schema.Struct fs -> do
          xss <- Cons.transpose <$> traverse (first TableValueSchemaError . Value.takeStruct) values1
          Struct
            <$> Cons.zipWithM fromField fs (fmap Cons.toVector xss)

        Schema.Nested tschema -> do
          xss <- traverse (first TableValueSchemaError . Value.takeNested) values
          uncurry Nested
            <$> fromNested tschema xss

        Schema.Reversed rschema -> do
          xss <- traverse (first TableValueSchemaError . Value.takeReversed) values
          Reversed
            <$> fromValues rschema xss

fromEnum ::
  Cons Boxed.Vector (Variant ColumnSchema) ->
  Boxed.Vector (Tag, Value) ->
  Either TableError (Cons Boxed.Vector (Variant Column))
fromEnum variants txs =
  Schema.forVariant variants $ \tag _ cschema ->
    fromValues cschema $
      Boxed.map (fromVariant cschema tag) txs

fromVariant :: ColumnSchema -> Tag -> (Tag, Value) -> Value
fromVariant cschema expectedTag (tag, value) =
  if expectedTag == tag then
    value
  else
    Value.defaultValue cschema

fromField :: Field ColumnSchema -> Boxed.Vector Value -> Either TableError (Field Column)
fromField field =
  fmap (field $>) .
  fromValues (Schema.field field)

------------------------------------------------------------------------
-- Table/Column -> Collection/Value

toCollection :: Table -> Either TableError Collection
toCollection = \case
  Binary bs ->
    pure $ Value.Binary bs

  Array c ->
    Value.Array <$> toValues c

  Map k v -> do
    ks <- toValues k
    vs <- toValues v
    pure . Value.Map . fromSorted $
      Boxed.zip ks vs

toNested :: Storable.Vector Int64 -> Table -> Either TableError (Boxed.Vector Collection)
toNested ns table =
  case table of
    Binary bs -> do
      bss <- first (TableNestedLengthMismatch $ schema table) $ Segment.reify ns bs
      pure $ fmap Value.Binary bss

    Array c -> do
      xs <- toValues c
      xss <- first (TableNestedLengthMismatch $ schema table) $ Segment.reify ns xs
      pure $ fmap Value.Array xss

    Map k v -> do
      kvs <- Generic.zip <$> toValues k <*> toValues v
      kvss <- first (TableNestedLengthMismatch $ schema table) $ Segment.reify ns kvs
      pure $ fmap (Value.Map . fromSorted) kvss

fromSorted :: Boxed.Vector (Value, Value) -> Map Value Value
fromSorted =
  Map.fromDistinctAscList . Boxed.toList

toValues :: Column -> Either TableError (Boxed.Vector Value)
toValues = \case
  Unit n ->
    pure $ Boxed.replicate n Value.Unit

  Int xs ->
    pure . fmap Value.Int $ Storable.convert xs

  Double xs ->
    pure . fmap Value.Double $ Storable.convert xs

  Enum tags0 vs0 -> do
    let
      tags =
        Storable.convert tags0

    values <- Cons.transposeCV <$> traverse (toValues . Schema.variant) vs0

    Boxed.zipWithM mkEnum tags values

  Struct fs ->
    fmap Value.Struct . Cons.transposeCV <$> traverse (toValues . Schema.field) fs

  Nested ns0 t -> do
    fmap Value.Nested <$> toNested ns0 t

  Reversed c ->
    fmap Value.Reversed <$> toValues c

mkEnum :: Tag -> Cons Boxed.Vector Value -> Either TableError Value
mkEnum tag values =
  case Cons.index (fromIntegral tag) values of
    Nothing ->
      Left $ TableNoValueForEnumTag tag values
    Just x ->
      pure $ Value.Enum tag x

------------------------------------------------------------------------
-- Splitting

splitAt :: Int -> Table -> (Table, Table)
splitAt i = \case
  Binary bs ->
    bimap Binary Binary $
      ByteString.splitAt i bs
  Array c ->
    bimap Array Array
      (splitAtColumn i c)
  Map k v ->
    biliftA2 Map Map
      (splitAtColumn i k)
      (splitAtColumn i v)

splitAtColumn :: Int -> Column -> (Column, Column)
splitAtColumn i = \case
  Unit n ->
    let
      m =
        min n (max 0 i)
    in
      (Unit m, Unit (n - m))

  Int xs ->
    bimap Int Int $
      Storable.splitAt i xs

  Double xs ->
    bimap Double Double $
      Storable.splitAt i xs

  Enum tags0 variants0 ->
    let
      tags =
        Storable.splitAt i tags0

      variants =
        fmap (fmap (splitAtColumn i)) variants0

      fst_variants =
        fmap (fmap fst) variants

      snd_variants =
        fmap (fmap snd) variants
    in
      biliftA2 Enum Enum tags (fst_variants, snd_variants)

  Struct fields0 ->
    let
      fields =
        fmap (fmap (splitAtColumn i)) fields0

      fst_fields =
        fmap (fmap fst) fields

      snd_fields =
        fmap (fmap snd) fields
    in
      (Struct fst_fields, Struct snd_fields)

  Nested ns table ->
    let
      (ns0, ns1) =
        Storable.splitAt i ns

      (table0, table1) =
        splitAt (fromIntegral $ Storable.sum ns0) table
    in
      (Nested ns0 table0, Nested ns1 table1)

  Reversed column ->
    bimap Reversed Reversed
      (splitAtColumn i column)

------------------------------------------------------------------------
-- Union

-- | /O(sorry)/
unions :: Cons Boxed.Vector Table -> Either TableError Table
unions xss = do
  vss <- Cons.mapM toCollection xss
  vs <- first TableValueUnionError $ Cons.fold1M' Value.union vss
  fromCollection (schema $ Cons.head xss) vs

-- | /O(no)/
union :: Table -> Table -> Either TableError Table
union x0 x1 = do
  c0 <- toCollection x0
  c1 <- toCollection x1
  c2 <- first TableValueUnionError $ Value.union c0 c1
  fromCollection (schema x0) c2

------------------------------------------------------------------------
-- Concat/Append
--
--   These are unsafe because they don't union maps, so tables can be
--   corrupted.
--

-- | /O(sorry)/
unsafeConcat :: Cons Boxed.Vector Table -> Either TableError Table
unsafeConcat =
  Cons.fold1M' unsafeAppend

-- | /O(n)/
unsafeAppend :: Table -> Table -> Either TableError Table
unsafeAppend x0 x1 =
  case (x0, x1) of
    (Binary bs0, Binary bs1) ->
      pure $ Binary (bs0 <> bs1)

    (Array xs0, Array xs1) ->
      Array
        <$> unsafeAppendColumn xs0 xs1

    (Map ks0 vs0, Map ks1 vs1) ->
      Map
        <$> unsafeAppendColumn ks0 ks1
        <*> unsafeAppendColumn vs0 vs1

    _ ->
      Left $ TableAppendTableMismatch (schema x0) (schema x1)

unsafeAppendColumn :: Column -> Column -> Either TableError Column
unsafeAppendColumn x0 x1 =
  case (x0, x1) of
    (Unit n0, Unit n1) ->
      pure $ Unit (n0 + n1)

    (Int xs0, Int xs1) ->
      pure $ Int (xs0 <> xs1)

    (Double xs0, Double xs1) ->
      pure $ Double (xs0 <> xs1)

    (Enum tags0 vs0, Enum tags1 vs1)
      | Cons.length vs0 == Cons.length vs1
      ->
        Enum (tags0 <> tags1) <$> Cons.zipWithM unsafeAppendVariant vs0 vs1

    (Struct fs0, Struct fs1)
      | Cons.length fs0 == Cons.length fs1
      ->
        Struct <$> Cons.zipWithM unsafeAppendField fs0 fs1

    (Nested ns0 t0, Nested ns1 t1) ->
      Nested (ns0 <> ns1) <$> unsafeAppend t0 t1

    (Reversed c0, Reversed c1) ->
      Reversed <$> unsafeAppendColumn c0 c1

    _ ->
      Left $ TableAppendColumnMismatch (schemaColumn x0) (schemaColumn x1)

unsafeAppendVariant :: Variant Column -> Variant Column -> Either TableError (Variant Column)
unsafeAppendVariant v0 v1 =
  if Schema.variantName v0 == Schema.variantName v1 then
    (v0 $>) <$> unsafeAppendColumn (Schema.variant v0) (Schema.variant v1)
  else
    Left $ TableAppendVariantMismatch (fmap schemaColumn v0) (fmap schemaColumn v1)

unsafeAppendField :: Field Column -> Field Column -> Either TableError (Field Column)
unsafeAppendField f0 f1 =
  if Schema.fieldName f0 == Schema.fieldName f1 then
    (f0 $>) <$> unsafeAppendColumn (Schema.field f0) (Schema.field f1)
  else
    Left $ TableAppendFieldMismatch (fmap schemaColumn f0) (fmap schemaColumn f1)
