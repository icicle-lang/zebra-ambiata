{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Striped (
    Table(..)
  , Column(..)

  , StripedError(..)
  , renderStripedError

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

  , fromLogical
  , fromValues

  , toLogical
  , toValues

  , splitAt
  , splitAtColumn

  , merges
  , merge

  , defaultTable
  , defaultColumn

  , transmute
  , transmuteColumn
  , transmuteStruct

  , unsafeConcat
  , unsafeAppend
  , unsafeAppendColumn

  -- * Streaming Operations
  , rechunk
  ) where

import           Control.Monad.Trans.Class (lift)

import           Data.Biapplicative (biliftA2)
import qualified Data.ByteString as ByteString
import           Data.ByteString.Internal (ByteString(..))
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text

import           GHC.Generics (Generic)

import           P hiding (empty, concat, splitAt, length)

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (EitherT, hoistEither)
import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons
import qualified X.Data.Vector.Generic as Generic

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaError(..))
import qualified Zebra.Table.Schema as Schema
import           Zebra.X.Stream (Stream, Of)
import qualified Zebra.X.Stream as Stream
import           Zebra.X.Vector.Segment (SegmentError)
import qualified Zebra.X.Vector.Segment as Segment
import qualified Zebra.X.Vector.Storable as Storable


data Table =
    Binary !Default !(Maybe Encoding.Binary) !ByteString
  | Array !Default !Column
  | Map !Default !Column !Column
    deriving (Eq, Ord, Show, Generic)

data Column =
    Unit !Int
  | Int !Default !(Storable.Vector Int64)
  | Double !Default !(Storable.Vector Double)
  | Enum !Default !(Storable.Vector Tag) !(Cons Boxed.Vector (Variant Column))
  | Struct !Default !(Cons Boxed.Vector (Field Column))
  | Nested !(Storable.Vector Int64) !Table
  | Reversed !Column
    deriving (Eq, Ord, Show, Generic)

data StripedError =
    StripedLogicalSchemaError !LogicalSchemaError
  | StripedLogicalMergeError !LogicalMergeError
  | StripedNoValueForEnumTag !Tag !(Cons Boxed.Vector Logical.Value)
  | StripedNestedLengthMismatch !Schema.Table !SegmentError
  | StripedDefaultFieldNotAllowed !(Field Schema.Column)
  | StripedTransmuteMapKeyNotAllowed !Schema.Table !Schema.Table
  | StripedTransmuteTableMismatch !Schema.Table !Schema.Table
  | StripedTransmuteColumnMismatch !Schema.Column !Schema.Column
  | StripedAppendTableMismatch !Schema.Table !Schema.Table
  | StripedAppendColumnMismatch !Schema.Column !Schema.Column
  | StripedAppendVariantMismatch !(Variant Schema.Column) !(Variant Schema.Column)
  | StripedAppendFieldMismatch !(Field Schema.Column) !(Field Schema.Column)
    deriving (Eq, Show)

renderStripedError :: StripedError -> Text
renderStripedError = \case
  StripedLogicalSchemaError err ->
    Logical.renderLogicalSchemaError err

  StripedLogicalMergeError err ->
    Logical.renderLogicalMergeError err

  StripedNoValueForEnumTag tag values ->
    "Cannot construct enum with <" <>
    Text.pack (show tag) <>
    "> from values: " <>
    Text.pack (ppShow values)

  StripedNestedLengthMismatch ts err ->
    "Failed to split table: " <> Text.pack (ppShow ts) <>
    "\n" <> Segment.renderSegmentError err

  StripedDefaultFieldNotAllowed (Field name value) ->
    "Schema did not allow defaulting of struct field:" <>
    ppField (unFieldName name) value

  StripedTransmuteMapKeyNotAllowed x y ->
    "Cannot transmute the key of a map, it could invalidate the ordering invariant:" <>
    ppField "actual" x <>
    ppField "desired" y

  StripedTransmuteTableMismatch x y ->
    "Cannot transmute table from actual schema to desired schema:" <>
    ppField "actual" x <>
    ppField "desired" y

  StripedTransmuteColumnMismatch x y ->
    "Cannot transmute column from actual schema to desired schema:" <>
    ppField "actual" x <>
    ppField "desired" y

  StripedAppendTableMismatch x y ->
    "Cannot append tables with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  StripedAppendColumnMismatch x y ->
    "Cannot append columns with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  StripedAppendVariantMismatch x y ->
    "Cannot append variants with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

  StripedAppendFieldMismatch x y ->
    "Cannot append fields with different schemas:" <>
    ppField "first" x <>
    ppField "second" y

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
  Binary _ _ bs ->
    ByteString.length bs
  Array _ c ->
    lengthColumn c
  Map _ k v ->
    -- FIXME This is doing more work than required, would
    -- FIXME be good to have a 'validate :: Table -> Bool'
    -- FIXME function that checks a table has matching
    -- FIXME lengths instead perhaps?
    min (lengthColumn k) (lengthColumn v)
{-# INLINABLE length #-}

lengthColumn :: Column -> Int
lengthColumn = \case
  Unit n ->
    n
  Int _ xs ->
    Storable.length xs
  Double _ xs ->
    Storable.length xs
  Enum _ tags _ ->
    Storable.length tags
  Struct _ fs ->
    lengthColumn . fieldData $ Cons.head fs
  Nested ns _ ->
    Storable.length ns
  Reversed c ->
    lengthColumn c
{-# INLINABLE lengthColumn #-}

schema :: Table -> Schema.Table
schema = \case
  Binary def encoding _ ->
    Schema.Binary def encoding
  Array def x ->
    Schema.Array def (schemaColumn x)
  Map def k v ->
    Schema.Map def (schemaColumn k) (schemaColumn v)
{-# INLINABLE schema #-}

schemaColumn :: Column -> Schema.Column
schemaColumn = \case
  Unit _ ->
    Schema.Unit
  Int def _ ->
    Schema.Int def
  Double def _ ->
    Schema.Double def
  Enum def _ vs ->
    Schema.Enum def (fmap (fmap schemaColumn) vs)
  Struct def fs ->
    Schema.Struct def (fmap (fmap schemaColumn) fs)
  Nested _ t ->
    Schema.Nested (schema t)
  Reversed c ->
    Schema.Reversed (schemaColumn c)
{-# INLINABLE schemaColumn #-}

------------------------------------------------------------------------

empty :: Schema.Table -> Table
empty = \case
  Schema.Binary def encoding ->
    Binary def encoding ByteString.empty
  Schema.Array def x ->
    Array def (emptyColumn x)
  Schema.Map def k v ->
    Map def (emptyColumn k) (emptyColumn v)
{-# INLINABLE empty #-}

emptyColumn :: Schema.Column -> Column
emptyColumn = \case
  Schema.Unit ->
    Unit 0
  Schema.Int def ->
    Int def Storable.empty
  Schema.Double def ->
    Double def Storable.empty
  Schema.Enum def vs ->
    Enum def Storable.empty (fmap (fmap emptyColumn) vs)
  Schema.Struct def fs ->
    Struct def (fmap (fmap emptyColumn) fs)
  Schema.Nested t ->
    Nested Storable.empty (empty t)
  Schema.Reversed c ->
    Reversed (emptyColumn c)
{-# INLINABLE emptyColumn #-}

------------------------------------------------------------------------

takeBinary :: Table -> Either SchemaError (Default, Maybe Encoding.Binary, ByteString)
takeBinary = \case
  Binary def encoding x ->
    Right (def, encoding, x)
  x ->
    Left $ SchemaExpectedBinary (schema x)
{-# INLINE takeBinary #-}

takeArray :: Table -> Either SchemaError (Default, Column)
takeArray = \case
  Array def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedArray (schema x)
{-# INLINE takeArray #-}

takeMap :: Table -> Either SchemaError (Default, Column, Column)
takeMap = \case
  Map def k v ->
    Right (def, k, v)
  x ->
    Left $ SchemaExpectedMap (schema x)
{-# INLINE takeMap #-}

takeInt :: Column -> Either SchemaError (Default, Storable.Vector Int64)
takeInt = \case
  Int def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedInt (schemaColumn x)
{-# INLINE takeInt #-}

takeDouble :: Column -> Either SchemaError (Default, Storable.Vector Double)
takeDouble = \case
  Double def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedDouble (schemaColumn x)
{-# INLINE takeDouble #-}

takeEnum :: Column -> Either SchemaError (Default, Storable.Vector Tag, Cons Boxed.Vector (Variant Column))
takeEnum = \case
  Enum def tags x ->
    Right (def, tags, x)
  x ->
    Left $ SchemaExpectedEnum (schemaColumn x)
{-# INLINE takeEnum #-}

takeStruct :: Column -> Either SchemaError (Default, Cons Boxed.Vector (Field Column))
takeStruct = \case
  Struct def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedStruct (schemaColumn x)
{-# INLINE takeStruct #-}

takeNested :: Column -> Either SchemaError (Storable.Vector Int64, Table)
takeNested = \case
  Nested ns x ->
    Right (ns, x)
  x ->
    Left $ SchemaExpectedNested (schemaColumn x)
{-# INLINE takeNested #-}

takeReversed :: Column -> Either SchemaError Column
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ SchemaExpectedReversed (schemaColumn x)
{-# INLINE takeReversed #-}

------------------------------------------------------------------------
-- Logical -> Striped

fromLogical :: Schema.Table -> Logical.Table -> Either StripedError Table
fromLogical tschema collection =
  case tschema of
    Schema.Binary def encoding ->
      Binary def encoding
        <$> first StripedLogicalSchemaError (Logical.takeBinary collection)

    Schema.Array def eschema -> do
      xs <- first StripedLogicalSchemaError $ Logical.takeArray collection
      Array def
        <$> fromValues eschema xs

    Schema.Map def kschema vschema -> do
      kvs <- first StripedLogicalSchemaError $ Logical.takeMap collection
      Map def
        <$> fromValues kschema (Boxed.fromList $ Map.keys kvs)
        <*> fromValues vschema (Boxed.fromList $ Map.elems kvs)
{-# INLINABLE fromLogical #-}

fromNested :: Schema.Table -> Boxed.Vector Logical.Table -> Either StripedError (Storable.Vector Int64, Table)
fromNested tschema xss0 =
  case tschema of
    Schema.Binary def encoding -> do
      bss <- first StripedLogicalSchemaError $ traverse Logical.takeBinary xss0
      pure (
          Storable.convert $ fmap (fromIntegral . ByteString.length) bss
        , Binary def encoding . ByteString.concat $ Boxed.toList bss
        )

    Schema.Array def eschema -> do
      xss <- first StripedLogicalSchemaError $ traverse Logical.takeArray xss0
      column <- fromValues eschema . Boxed.concat $ Boxed.toList xss
      pure (
          Storable.convert $ fmap (fromIntegral . Boxed.length) xss
        , Array def column
        )

    Schema.Map def kschema vschema -> do
      kvss <- first StripedLogicalSchemaError $ traverse Logical.takeMap xss0

      let
        (ks0, vs0) =
          Boxed.unzip $ Boxed.concatMap (Boxed.fromList . Map.toList) kvss

      ks <- fromValues kschema ks0
      vs <- fromValues vschema vs0

      pure (
          Storable.convert $ fmap (fromIntegral . Map.size) kvss
        , Map def ks vs
        )
{-# INLINABLE fromNested #-}

fromValues :: Schema.Column -> Boxed.Vector Logical.Value -> Either StripedError Column
fromValues cschema values =
  case Cons.fromVector values of
    Nothing ->
      pure $ emptyColumn cschema
    Just values1 ->
      case cschema of
        Schema.Unit ->
          pure . Unit $ Boxed.length values

        Schema.Int def ->
          Int def . Storable.convert
            <$> traverse (first StripedLogicalSchemaError . Logical.takeInt) values

        Schema.Double def ->
          Double def . Storable.convert
            <$> traverse (first StripedLogicalSchemaError . Logical.takeDouble) values

        Schema.Enum def vs -> do
          txs <- traverse (first StripedLogicalSchemaError . Logical.takeEnum) values

          let
            tags =
              Storable.convert $ fmap (fromIntegral . fst) txs

          Enum def
            <$> pure tags
            <*> fromEnum vs txs

        Schema.Struct def fs -> do
          xss <- Cons.transpose <$> traverse (first StripedLogicalSchemaError . Logical.takeStruct) values1
          Struct def
            <$> Cons.zipWithM fromField fs (fmap Cons.toVector xss)

        Schema.Nested tschema -> do
          xss <- traverse (first StripedLogicalSchemaError . Logical.takeNested) values
          uncurry Nested
            <$> fromNested tschema xss

        Schema.Reversed rschema -> do
          xss <- traverse (first StripedLogicalSchemaError . Logical.takeReversed) values
          Reversed
            <$> fromValues rschema xss
{-# INLINABLE fromValues #-}

fromEnum ::
     Cons Boxed.Vector (Variant Schema.Column)
  -> Boxed.Vector (Tag, Logical.Value)
  -> Either StripedError (Cons Boxed.Vector (Variant Column))
fromEnum variants txs =
  forVariant variants $ \tag _ cschema ->
    fromValues cschema $
      Boxed.map (fromVariant cschema tag) txs
{-# INLINABLE fromEnum #-}

fromVariant :: Schema.Column -> Tag -> (Tag, Logical.Value) -> Logical.Value
fromVariant cschema expectedTag (tag, value) =
  if expectedTag == tag then
    value
  else
    Logical.defaultValue cschema
{-# INLINABLE fromVariant #-}

fromField :: Field Schema.Column -> Boxed.Vector Logical.Value -> Either StripedError (Field Column)
fromField field =
  fmap (field $>) .
  fromValues (fieldData field)
{-# INLINABLE fromField #-}

------------------------------------------------------------------------
-- Striped -> Logical

toLogical :: Table -> Either StripedError Logical.Table
toLogical = \case
  Binary _ _ bs ->
    pure $ Logical.Binary bs

  Array _ x ->
    Logical.Array <$> toValues x

  Map _ k v -> do
    ks <- toValues k
    vs <- toValues v
    pure . Logical.Map . fromSorted $
      Boxed.zip ks vs
{-# INLINABLE toLogical #-}

toNested :: Storable.Vector Int64 -> Table -> Either StripedError (Boxed.Vector Logical.Table)
toNested ns table =
  case table of
    Binary _ _ bs -> do
      bss <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns bs
      pure $ fmap Logical.Binary bss

    Array _ x -> do
      xs <- toValues x
      xss <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns xs
      pure $ fmap Logical.Array xss

    Map _ k v -> do
      kvs <- Generic.zip <$> toValues k <*> toValues v
      kvss <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns kvs
      pure $ fmap (Logical.Map . fromSorted) kvss
{-# INLINABLE toNested #-}

fromSorted :: Boxed.Vector (Logical.Value, Logical.Value) -> Map Logical.Value Logical.Value
fromSorted =
  -- FIXME Check order and error if not sorted
  Map.fromDistinctAscList . Boxed.toList
{-# INLINABLE fromSorted #-}

toValues :: Column -> Either StripedError (Boxed.Vector Logical.Value)
toValues = \case
  Unit n ->
    pure $ Boxed.replicate n Logical.Unit

  Int _ xs ->
    pure . fmap Logical.Int $ Storable.convert xs

  Double _ xs ->
    pure . fmap Logical.Double $ Storable.convert xs

  Enum _ tags0 vs0 -> do
    let
      tags =
        Storable.convert tags0

    values <- Cons.transposeCV <$> traverse (toValues . variantData) vs0

    Boxed.zipWithM mkEnum tags values

  Struct _ fs ->
    fmap Logical.Struct . Cons.transposeCV <$> traverse (toValues . fieldData) fs

  Nested ns0 t -> do
    fmap Logical.Nested <$> toNested ns0 t

  Reversed c ->
    fmap Logical.Reversed <$> toValues c
{-# INLINABLE toValues #-}

mkEnum :: Tag -> Cons Boxed.Vector Logical.Value -> Either StripedError Logical.Value
mkEnum tag values =
  case Cons.index (fromIntegral tag) values of
    Nothing ->
      Left $ StripedNoValueForEnumTag tag values
    Just x ->
      pure $ Logical.Enum tag x
{-# INLINABLE mkEnum #-}

------------------------------------------------------------------------
-- Splitting

splitAt :: Int -> Table -> (Table, Table)
splitAt i = \case
  Binary def encoding bs ->
    bimap (Binary def encoding) (Binary def encoding) $
      ByteString.splitAt i bs

  Array def x ->
    bimap (Array def) (Array def)
      (splitAtColumn i x)

  Map def k v ->
    biliftA2 (Map def) (Map def)
      (splitAtColumn i k)
      (splitAtColumn i v)
{-# INLINABLE splitAt #-}

splitAtColumn :: Int -> Column -> (Column, Column)
splitAtColumn i = \case
  Unit n ->
    let
      m =
        min n (max 0 i)
    in
      (Unit m, Unit (n - m))

  Int def xs ->
    bimap (Int def) (Int def) $
      Storable.splitAt i xs

  Double def xs ->
    bimap (Double def) (Double def) $
      Storable.splitAt i xs

  Enum def tags0 variants0 ->
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
      biliftA2 (Enum def) (Enum def) tags (fst_variants, snd_variants)

  Struct def fields0 ->
    let
      fields =
        fmap (fmap (splitAtColumn i)) fields0

      fst_fields =
        fmap (fmap fst) fields

      snd_fields =
        fmap (fmap snd) fields
    in
      (Struct def fst_fields, Struct def snd_fields)

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
{-# INLINABLE splitAtColumn #-}

------------------------------------------------------------------------
-- Merge

-- | /O(sorry)/
merges :: Cons Boxed.Vector Table -> Either StripedError Table
merges xss = do
  vss <- Cons.mapM toLogical xss
  vs <- first StripedLogicalMergeError $ Cons.fold1M' Logical.merge vss
  fromLogical (schema $ Cons.head xss) vs
{-# INLINABLE merges #-}

-- | /O(no)/
merge :: Table -> Table -> Either StripedError Table
merge x0 x1 = do
  c0 <- toLogical x0
  c1 <- toLogical x1
  c2 <- first StripedLogicalMergeError $ Logical.merge c0 c1
  fromLogical (schema x0) c2
{-# INLINABLE merge #-}

------------------------------------------------------------------------
-- Default

defaultTable :: Schema.Table -> Table
defaultTable = \case
  Schema.Binary def encoding ->
    Binary def encoding ""
  Schema.Array def x ->
    Array def (defaultColumn 0 x)
  Schema.Map def k v ->
    Map def (defaultColumn 0 k) (defaultColumn 0 v)
{-# INLINABLE defaultTable #-}

defaultColumn :: Int -> Schema.Column -> Column
defaultColumn n = \case
  Schema.Unit ->
    Unit n
  Schema.Int def ->
    Int def (Storable.replicate n 0)
  Schema.Double def ->
    Double def (Storable.replicate n 0)
  Schema.Enum def vs ->
    Enum def (Storable.replicate n 0) (fmap2 (defaultColumn n) vs)
  Schema.Struct def fs ->
    Struct def (fmap2 (defaultColumn n) fs)
  Schema.Nested x ->
    Nested (Storable.replicate n 0) (defaultTable x)
  Schema.Reversed x ->
    Reversed (defaultColumn n x)
{-# INLINABLE defaultColumn #-}

------------------------------------------------------------------------
-- Transmute

--
-- FIXME should we allow more flexibility with mismatched defaults / encodings / enums?
--

-- | Modify a table's structure so that it has the desired schema.
--
--   This operation will only be successful if the original table had a
--   compatible structure. Compatible in this case means that it is only
--   allowed to be missing columns which can be set to a default value.
--
transmute :: Schema.Table -> Table -> Either StripedError Table
transmute s t =
  case (s, t) of
    (Schema.Binary def0 encoding0, Binary def1 encoding1 bs)
      | def0 == def1
      , encoding0 == encoding1
      ->
        pure $
          Binary def1 encoding1 bs

    (Schema.Array def0 column0, Array def1 column1)
      | def0 == def1
      ->
        Array def1
          <$> transmuteColumn column0 column1

    (Schema.Map def0 k0 v0, Map def1 k1 v1)
      | k0 /= schemaColumn k1
      ->
        Left $ StripedTransmuteMapKeyNotAllowed (schema t) s

      | def0 == def1
      ->
        Map def1
          <$> transmuteColumn k0 k1
          <*> transmuteColumn v0 v1

    _ ->
      Left $ StripedTransmuteTableMismatch (schema t) s
{-# INLINABLE transmute #-}

transmuteColumn :: Schema.Column -> Column -> Either StripedError Column
transmuteColumn s c =
  case (s, c) of
    (Schema.Unit, Unit n) ->
      pure $ Unit n

    (Schema.Int def0, Int def1 xs)
      | def0 == def1
      ->
        pure $ Int def1 xs

    (Schema.Double def0, Double def1 xs)
      | def0 == def1
      ->
        pure $ Double def1 xs

    (Schema.Enum def0 vs0, Enum def1 tags vs1)
      | def0 == def1
      , fmap variantName vs0 == fmap variantName vs1
      ->
        Enum def1 tags
          <$> Cons.zipWithM (\(Variant _ x) (Variant n y) -> Variant n <$> transmuteColumn x y) vs0 vs1

    (Schema.Struct def0 fs0, Struct def1 fs1)
      | def0 == def1
      ->
        Struct def1
          <$> transmuteStruct fs0 fs1

    (Schema.Nested xs0, Nested ns1 xs1) ->
      Nested ns1
        <$> transmute xs0 xs1

    (Schema.Reversed xs0, Reversed xs1) ->
      Reversed
        <$> transmuteColumn xs0 xs1

    _ ->
      Left $ StripedTransmuteColumnMismatch (schemaColumn c) s
{-# INLINABLE transmuteColumn #-}

transmuteStruct ::
     Cons Boxed.Vector (Field Schema.Column)
  -> Cons Boxed.Vector (Field Column)
  -> Either StripedError (Cons Boxed.Vector (Field Column))
transmuteStruct schemas columns0 =
  let
    n =
      lengthColumn . fieldData $ Cons.head columns0

    columns =
      Map.fromList .
      fmap (\(Field k v) -> (k, v)) $
      Cons.toList columns0

    lookupField field@(Field name fschema) =
      case Map.lookup name columns of
        Nothing ->
          case Schema.takeDefaultColumn fschema of
            DenyDefault ->
              Left $ StripedDefaultFieldNotAllowed field
            AllowDefault ->
              pure . Field name $ defaultColumn n fschema

        Just column ->
          Field name <$> transmuteColumn fschema column
  in
    traverse lookupField schemas
{-# INLINABLE transmuteStruct #-}

------------------------------------------------------------------------
-- Concat/Append
--
--   These are unsafe because they don't union maps, so tables can be
--   corrupted.
--

-- | /O(sorry)/
unsafeConcat :: Cons Boxed.Vector Table -> Either StripedError Table
unsafeConcat =
  Cons.fold1M' unsafeAppend
{-# INLINABLE unsafeConcat #-}

-- | /O(n)/
unsafeAppend :: Table -> Table -> Either StripedError Table
unsafeAppend x0 x1 =
  case (x0, x1) of
    (Binary def0 encoding0 bs0, Binary def1 encoding1 bs1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        pure $ Binary def0 encoding0 (bs0 <> bs1)

    (Array def0 xs0, Array def1 xs1)
      | def0 == def1
      ->
        Array def0
          <$> unsafeAppendColumn xs0 xs1

    (Map def0 ks0 vs0, Map def1 ks1 vs1)
      | def0 == def1
      ->
        Map def0
          <$> unsafeAppendColumn ks0 ks1
          <*> unsafeAppendColumn vs0 vs1

    _ ->
      Left $ StripedAppendTableMismatch (schema x0) (schema x1)
{-# INLINABLE unsafeAppend #-}

unsafeAppendColumn :: Column -> Column -> Either StripedError Column
unsafeAppendColumn x0 x1 =
  case (x0, x1) of
    (Unit n0, Unit n1) ->
      pure $ Unit (n0 + n1)

    (Int def0 xs0, Int def1 xs1)
      | def0 == def1
      ->
        pure $ Int def0 (xs0 <> xs1)

    (Double def0 xs0, Double def1 xs1)
      | def0 == def1
      ->
        pure $ Double def0 (xs0 <> xs1)

    (Enum def0 tags0 vs0, Enum def1 tags1 vs1)
      | def0 == def1
      , Cons.length vs0 == Cons.length vs1
      ->
        Enum def0 (tags0 <> tags1) <$> Cons.zipWithM unsafeAppendVariant vs0 vs1

    (Struct def0 fs0, Struct def1 fs1)
      | def0 == def1
      , Cons.length fs0 == Cons.length fs1
      ->
        Struct def0 <$> Cons.zipWithM unsafeAppendField fs0 fs1

    (Nested ns0 t0, Nested ns1 t1) ->
      Nested (ns0 <> ns1) <$> unsafeAppend t0 t1

    (Reversed c0, Reversed c1) ->
      Reversed <$> unsafeAppendColumn c0 c1

    _ ->
      Left $ StripedAppendColumnMismatch (schemaColumn x0) (schemaColumn x1)
{-# INLINABLE unsafeAppendColumn #-}

unsafeAppendVariant :: Variant Column -> Variant Column -> Either StripedError (Variant Column)
unsafeAppendVariant v0 v1 =
  if variantName v0 == variantName v1 then
    (v0 $>) <$> unsafeAppendColumn (variantData v0) (variantData v1)
  else
    Left $ StripedAppendVariantMismatch (fmap schemaColumn v0) (fmap schemaColumn v1)
{-# INLINABLE unsafeAppendVariant #-}

unsafeAppendField :: Field Column -> Field Column -> Either StripedError (Field Column)
unsafeAppendField f0 f1 =
  if fieldName f0 == fieldName f1 then
    (f0 $>) <$> unsafeAppendColumn (fieldData f0) (fieldData f1)
  else
    Left $ StripedAppendFieldMismatch (fmap schemaColumn f0) (fmap schemaColumn f1)
{-# INLINABLE unsafeAppendField #-}

------------------------------------------------------------------------
-- Streaming Operations

data Yield =
    YieldedNone
  | YieldedSome
    deriving (Eq)

data Rechunk =
  Rechunk {
      _rechunkCount :: !Int
    , _rechunkYield :: !Yield
    , _rechunkDList :: [Table] -> [Table]
    }

-- | Takes a stream of tables and rearranges them such that each chunk contains
--   the specified number of rows.
--
--   /The last chunk may contain fewer rows than the specified row count./
--
rechunk ::
     forall m r.
     Monad m
  => Int
  -> Stream (Of Table) m r
  -> Stream (Of Table) (EitherT StripedError m)  r
rechunk max_n =
  let
    loop :: Rechunk -> Stream (Of Table) m r -> Stream (Of Table) (EitherT StripedError m) r
    loop (Rechunk n0 emit dl) incoming0 =
      if n0 >= max_n then do
        x <- lift . hoistEither $ unsafeFromDList dl []

        let
          (x0, x1) =
            splitAt max_n x

        Stream.yield x0
        loop (Rechunk (length x1) YieldedSome (x1 :)) incoming0

      else do
        ex <- lift . lift $ Stream.next incoming0
        case ex of
          Left r -> do
            x <- lift . hoistEither $ unsafeFromDList dl []

            -- We need to yield a block if it contains any data, or if we have
            -- never yielded a block before. Streams of striped tables should
            -- contain at least one table, even if it is empty, because it
            -- carries schema information and makes the stream self-describing.
            when (n0 /= 0 || emit == YieldedNone) $
              Stream.yield x

            pure r

          Right (hd, tl) ->
            loop (Rechunk (n0 + length hd) emit (dl . (hd :))) tl
  in
    loop (Rechunk 0 YieldedNone id)
{-# INLINABLE rechunk #-}

unsafeFromDList :: ([Table] -> [Table]) -> [Table] -> Either StripedError Table
unsafeFromDList dl end =
  unsafeConcat . Cons.unsafeFromList $ dl end
{-# INLINE unsafeFromDList #-}
