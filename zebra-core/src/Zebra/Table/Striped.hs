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

  -- * Summary
  , length

  , schema
  , schemaColumn

  -- * Construction
  , empty
  , emptyColumn

  , defaultTable
  , defaultColumn

  -- * Destruction
  , takeBinary
  , takeArray
  , takeMap
  , takeInt
  , takeDouble
  , takeEnum
  , takeStruct
  , takeNested
  , takeReversed

  -- * Conversion
  , fromLogical
  , fromValues

  , toLogical
  , toValues

  -- * Slicing
  , splitAt
  , splitAtColumn

  -- * Alchemy
  , transmute
  , transmuteColumn
  , transmuteStruct

  -- * Merging
  , merges
  , merge

  , unsafeConcat
  , unsafeAppend
  , unsafeAppendColumn

  -- * Streaming
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

import qualified Neutron.Vector.Boxed as Boxed
import           Neutron.Vector.Cons (Cons)
import qualified Neutron.Vector.Cons as Cons
import qualified Neutron.Vector.Generic as Generic

import           P hiding (empty, concat, splitAt, length)

import           Text.Show.Pretty (ppShow)

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither)

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaError(..), SchemaUnionError)
import qualified Zebra.Table.Schema as Schema
import           Zebra.X.Vector.Segment (SegmentError)
import qualified Zebra.X.Vector.Segment as Segment
import qualified Zebra.X.Vector.Storable as Storable (Vector)


data Table =
    Binary !Default !Encoding.Binary !ByteString
  | Array !Default !Column
  | Map !Default !Column !Column
    deriving (Eq, Ord, Show, Generic)

instance NFData Table

data Column =
    Unit !Int
  | Int !Default !Encoding.Int !(Storable.Vector Int64)
  | Double !Default !(Storable.Vector Double)
  | Enum !Default !(Storable.Vector Tag) !(Cons Boxed.Vector (Variant Column))
  | Struct !Default !(Cons Boxed.Vector (Field Column))
  | Nested !(Storable.Vector Int64) !Table
  | Reversed !Column
    deriving (Eq, Ord, Show, Generic)

instance NFData Column

data StripedError =
    StripedLogicalSchemaError !LogicalSchemaError
  | StripedLogicalMergeError !LogicalMergeError
  | StripedFieldCountMismatch !Int !(Cons Boxed.Vector (Field (Schema.Column)))
  | StripedNoValueForEnumTag !Tag !(Cons Boxed.Vector Logical.Value)
  | StripedNestedLengthMismatch !Schema.Table !SegmentError
  | StripedMapNotSorted !Logical.Value !Logical.Value
  | StripedDefaultFieldNotAllowed !(Field Schema.Column)
  | StripedSchemaUnionError !SchemaUnionError
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

  StripedFieldCountMismatch n fields ->
    "Cannot convert from logical struct with <" <>
    Text.pack (show n) <>
    "> fields, schema had <" <>
    Text.pack (show (Cons.length fields)) <>
    ">: " <>
    ppField "fields" fields

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

  StripedMapNotSorted prev next ->
    "Table corrupt, found map which was not sorted:" <>
    ppField "current-key" prev <>
    ppField "next-key" next

  StripedSchemaUnionError err ->
    Schema.renderSchemaUnionError err

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
length = {-# SCC length #-} \case
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
lengthColumn = {-# SCC lengthColumn #-} \case
  Unit n ->
    n
  Int _ _ xs ->
    Generic.length xs
  Double _ xs ->
    Generic.length xs
  Enum _ tags _ ->
    Generic.length tags
  Struct _ fs ->
    lengthColumn . fieldData $ Cons.head fs
  Nested ns _ ->
    Generic.length ns
  Reversed c ->
    lengthColumn c
{-# INLINABLE lengthColumn #-}

schema :: Table -> Schema.Table
schema = {-# SCC schema #-} \case
  Binary def encoding _ ->
    Schema.Binary def encoding
  Array def x ->
    Schema.Array def (schemaColumn x)
  Map def k v ->
    Schema.Map def (schemaColumn k) (schemaColumn v)
{-# INLINABLE schema #-}

schemaColumn :: Column -> Schema.Column
schemaColumn = {-# SCC schemaColumn #-} \case
  Unit _ ->
    Schema.Unit
  Int def encoding _ ->
    Schema.Int def encoding
  Double def _ ->
    Schema.Double def
  Enum def _ vs ->
    Schema.Enum def (Cons.map (fmap schemaColumn) vs)
  Struct def fs ->
    Schema.Struct def (Cons.map (fmap schemaColumn) fs)
  Nested _ t ->
    Schema.Nested (schema t)
  Reversed c ->
    Schema.Reversed (schemaColumn c)
{-# INLINABLE schemaColumn #-}

------------------------------------------------------------------------

empty :: Schema.Table -> Table
empty = {-# SCC empty #-} \case
  Schema.Binary def encoding ->
    Binary def encoding ByteString.empty
  Schema.Array def x ->
    Array def (emptyColumn x)
  Schema.Map def k v ->
    Map def (emptyColumn k) (emptyColumn v)
{-# INLINABLE empty #-}

emptyColumn :: Schema.Column -> Column
emptyColumn = {-# SCC emptyColumn #-} \case
  Schema.Unit ->
    Unit 0
  Schema.Int def encoding ->
    Int def encoding Generic.empty
  Schema.Double def ->
    Double def Generic.empty
  Schema.Enum def vs ->
    Enum def Generic.empty (Cons.map (fmap emptyColumn) vs)
  Schema.Struct def fs ->
    Struct def (Cons.map (fmap emptyColumn) fs)
  Schema.Nested t ->
    Nested Generic.empty (empty t)
  Schema.Reversed c ->
    Reversed (emptyColumn c)
{-# INLINABLE emptyColumn #-}

------------------------------------------------------------------------

takeBinary :: Table -> Either SchemaError (Default, Encoding.Binary, ByteString)
takeBinary = {-# SCC takeBinary #-} \case
  Binary def encoding x ->
    Right (def, encoding, x)
  x ->
    Left $ SchemaExpectedBinary (schema x)
{-# INLINE takeBinary #-}

takeArray :: Table -> Either SchemaError (Default, Column)
takeArray = {-# SCC takeArray #-} \case
  Array def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedArray (schema x)
{-# INLINE takeArray #-}

takeMap :: Table -> Either SchemaError (Default, Column, Column)
takeMap = {-# SCC takeMap #-} \case
  Map def k v ->
    Right (def, k, v)
  x ->
    Left $ SchemaExpectedMap (schema x)
{-# INLINE takeMap #-}

takeInt :: Column -> Either SchemaError (Default, Encoding.Int, Storable.Vector Int64)
takeInt = {-# SCC takeInt #-} \case
  Int def encoding x ->
    Right (def, encoding, x)
  x ->
    Left $ SchemaExpectedInt (schemaColumn x)
{-# INLINE takeInt #-}

takeDouble :: Column -> Either SchemaError (Default, Storable.Vector Double)
takeDouble = {-# SCC takeDouble #-} \case
  Double def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedDouble (schemaColumn x)
{-# INLINE takeDouble #-}

takeEnum :: Column -> Either SchemaError (Default, Storable.Vector Tag, Cons Boxed.Vector (Variant Column))
takeEnum = {-# SCC takeEnum #-} \case
  Enum def tags x ->
    Right (def, tags, x)
  x ->
    Left $ SchemaExpectedEnum (schemaColumn x)
{-# INLINE takeEnum #-}

takeStruct :: Column -> Either SchemaError (Default, Cons Boxed.Vector (Field Column))
takeStruct = {-# SCC takeStruct #-} \case
  Struct def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedStruct (schemaColumn x)
{-# INLINE takeStruct #-}

takeNested :: Column -> Either SchemaError (Storable.Vector Int64, Table)
takeNested = {-# SCC takeNested #-} \case
  Nested ns x ->
    Right (ns, x)
  x ->
    Left $ SchemaExpectedNested (schemaColumn x)
{-# INLINE takeNested #-}

takeReversed :: Column -> Either SchemaError Column
takeReversed = {-# SCC takeReversed #-} \case
  Reversed x ->
    Right x
  x ->
    Left $ SchemaExpectedReversed (schemaColumn x)
{-# INLINE takeReversed #-}

------------------------------------------------------------------------
-- Logical -> Striped

fromLogical :: Schema.Table -> Logical.Table -> Either StripedError Table
fromLogical tschema collection = {-# SCC fromLogical #-}
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
        <$> fromValues kschema (Generic.fromList $ Map.keys kvs)
        <*> fromValues vschema (Generic.fromList $ Map.elems kvs)
{-# INLINABLE fromLogical #-}

fromNested :: Schema.Table -> Boxed.Vector Logical.Table -> Either StripedError (Storable.Vector Int64, Table)
fromNested tschema xss0 = {-# SCC fromNested #-}
  case tschema of
    Schema.Binary def encoding -> do
      !(bss :: Boxed.Vector ByteString) <-
        first StripedLogicalSchemaError $ Generic.mapM Logical.takeBinary xss0

      let
        !xx =
          Generic.map (fromIntegral . ByteString.length) bss

        !yy =
          Binary def encoding . ByteString.concat $ Generic.toList bss

      return (xx, yy)

    Schema.Array def eschema -> do
      !(xss :: Boxed.Vector (Boxed.Vector Logical.Value)) <-
        first StripedLogicalSchemaError $ Generic.mapM Logical.takeArray xss0

      column <-
        fromValues eschema $ Boxed.concat xss

      return (
          Generic.map (fromIntegral . Generic.length) xss
        , Array def column
        )

    Schema.Map def kschema vschema -> do
      !(kvss :: Boxed.Vector (Map Logical.Value Logical.Value)) <-
        first StripedLogicalSchemaError $ Generic.mapM Logical.takeMap xss0

      let
        -- FIXME hand fuse?
        (ks0, vs0) =
          Generic.unzip .
          Boxed.concat $
          Generic.map (Generic.fromList . Map.toList) kvss

      ks <- fromValues kschema ks0
      vs <- fromValues vschema vs0

      return (
          Generic.map (fromIntegral . Map.size) kvss
        , Map def ks vs
        )
{-# INLINABLE fromNested #-}

fromValues :: Schema.Column -> Boxed.Vector Logical.Value -> Either StripedError Column
fromValues cschema values = {-# SCC fromValues #-}
  case Cons.fromVector values of
    Nothing ->
      return $ emptyColumn cschema
    Just values1 ->
      case cschema of
        Schema.Unit ->
          return . Unit $ Generic.length values

        Schema.Int def encoding ->
          Int def encoding
            <$> Generic.mapM (first StripedLogicalSchemaError . Logical.takeInt) values

        Schema.Double def ->
          Double def
            <$> Generic.mapM (first StripedLogicalSchemaError . Logical.takeDouble) values

        Schema.Enum def vs -> do
          txs <- Generic.mapM (first StripedLogicalSchemaError . Logical.takeEnum) values

          let
            tags =
              Generic.map (fromIntegral . fst) txs

          Enum def tags
            <$> fromEnum vs txs

        Schema.Struct def fs -> do
          xss <- Cons.transpose <$> Cons.mapM (first StripedLogicalSchemaError . Logical.takeStruct) values1

          when (Cons.length fs /= Cons.length xss) $
            Left $ StripedFieldCountMismatch (Cons.length xss) fs

          Struct def
            <$> Cons.zipWithM fromField fs (Cons.toConsVectorVector xss)

        Schema.Nested tschema -> do
          xss <- Generic.mapM (first StripedLogicalSchemaError . Logical.takeNested) values
          uncurry Nested
            <$> fromNested tschema xss

        Schema.Reversed rschema -> do
          xss <- Generic.mapM (first StripedLogicalSchemaError . Logical.takeReversed) values
          Reversed
            <$> fromValues rschema xss
{-# INLINABLE fromValues #-}

fromEnum ::
     Cons Boxed.Vector (Variant Schema.Column)
  -> Boxed.Vector (Tag, Logical.Value)
  -> Either StripedError (Cons Boxed.Vector (Variant Column))
fromEnum variants txs = {-# SCC fromEnum #-}
  forVariant variants $ \tag _ cschema ->
    fromValues cschema $
      Generic.map (fromVariant cschema tag) txs
{-# INLINABLE fromEnum #-}

fromVariant :: Schema.Column -> Tag -> (Tag, Logical.Value) -> Logical.Value
fromVariant cschema expectedTag (tag, value) = {-# SCC fromVariant #-}
  if expectedTag == tag then
    value
  else
    Logical.defaultValue cschema
{-# INLINABLE fromVariant #-}

fromField :: Field Schema.Column -> Boxed.Vector Logical.Value -> Either StripedError (Field Column)
fromField field = {-# SCC fromField #-}
  fmap (field $>) .
  fromValues (fieldData field)
{-# INLINABLE fromField #-}

------------------------------------------------------------------------
-- Striped -> Logical

toLogical :: Table -> Either StripedError Logical.Table
toLogical = {-# SCC toLogical #-} \case
  Binary _ _ bs ->
    return $ Logical.Binary bs

  Array _ x ->
    Logical.Array <$> toValues x

  Map _ k v -> do
    ks <- toValues k
    vs <- toValues v
    fmap Logical.Map . fromSorted $ Generic.zip ks vs
{-# INLINABLE toLogical #-}

toNested :: Storable.Vector Int64 -> Table -> Either StripedError (Boxed.Vector Logical.Table)
toNested ns table = {-# SCC toNested #-}
  case table of
    Binary _ _ bs -> do
      bss <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns bs
      return $ Generic.map Logical.Binary bss

    Array _ x -> do
      xs <- toValues x
      xss <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns xs
      return $ Generic.map Logical.Array xss

    Map _ k v -> do
      kvs <- Generic.zip <$> toValues k <*> toValues v
      kvss0 <- first (StripedNestedLengthMismatch $ schema table) $ Segment.reify ns kvs
      Generic.mapM (fmap Logical.Map . fromSorted) kvss0
{-# INLINABLE toNested #-}

ensureSorted :: Boxed.Vector (Logical.Value, Logical.Value) -> Either StripedError ()
ensureSorted kvs = {-# SCC ensureSorted #-}
  let
    loop (prev, _) (next, v) =
      if prev > next then
        Left $ StripedMapNotSorted prev next
      else
        return (next, v)
  in
    case Generic.fold1M_ loop kvs of
      Nothing ->
        return ()
      Just x ->
        x
{-# INLINABLE ensureSorted #-}

fromSorted :: Boxed.Vector (Logical.Value, Logical.Value) -> Either StripedError (Map Logical.Value Logical.Value)
fromSorted kvs = {-# SCC fromSorted #-} do
  ensureSorted kvs
  return . Map.fromDistinctAscList $ Generic.toList kvs
{-# INLINABLE fromSorted #-}

toValues :: Column -> Either StripedError (Boxed.Vector Logical.Value)
toValues = {-# SCC toValues #-} \case
  Unit n ->
    return $ Generic.replicate n Logical.Unit

  Int _ _ xs ->
    return $ Generic.map Logical.Int xs

  Double _ xs ->
    return $ Generic.map Logical.Double xs

  Enum _ tags vs -> do
    values <- Cons.transposeCV <$!> Cons.mapM (toValues . variantData) vs
    Generic.zipWithM mkEnum tags values

  Struct _ fs -> do
    values <- Cons.transposeCV <$!> Cons.mapM (toValues . fieldData) fs
    return $
      Generic.map Logical.Struct values

  Nested ns0 t -> do
    Generic.map Logical.Nested <$> toNested ns0 t

  Reversed c ->
    Generic.map Logical.Reversed <$> toValues c
{-# INLINABLE toValues #-}

mkEnum :: Tag -> Cons Boxed.Vector Logical.Value -> Either StripedError Logical.Value
mkEnum tag values = {-# SCC mkEnum #-}
  case Cons.index (fromIntegral tag) values of
    Nothing ->
      Left $ StripedNoValueForEnumTag tag values
    Just x ->
      return $ Logical.Enum tag x
{-# INLINABLE mkEnum #-}

------------------------------------------------------------------------
-- Splitting

splitAt :: Int -> Table -> (Table, Table)
splitAt i = {-# SCC splitAt #-} \case
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
splitAtColumn i = {-# SCC splitAtColumn #-} \case
  Unit n ->
    let
      m =
        min n (max 0 i)
    in
      (Unit m, Unit (n - m))

  Int def encoding xs ->
    bimap (Int def encoding) (Int def encoding) $
      Generic.splitAt i xs

  Double def xs ->
    bimap (Double def) (Double def) $
      Generic.splitAt i xs

  Enum def tags0 variants0 ->
    let
      tags =
        Generic.splitAt i tags0

      variants =
        Cons.map (fmap (splitAtColumn i)) variants0

      fst_variants =
        Cons.map (fmap fst) variants

      snd_variants =
        Cons.map (fmap snd) variants
    in
      biliftA2 (Enum def) (Enum def) tags (fst_variants, snd_variants)

  Struct def fields0 ->
    let
      fields =
        Cons.map (fmap (splitAtColumn i)) fields0

      fst_fields =
        Cons.map (fmap fst) fields

      snd_fields =
        Cons.map (fmap snd) fields
    in
      (Struct def fst_fields, Struct def snd_fields)

  Nested ns table ->
    let
      (ns0, ns1) =
        Generic.splitAt i ns

      (table0, table1) =
        splitAt (fromIntegral $ Generic.sum ns0) table
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
merges xss = {-# SCC merges #-} do
  s <- first StripedSchemaUnionError . Cons.fold1M Schema.union $ Cons.map schema xss

  vss <- Cons.mapM (bind toLogical . transmute s) xss
  vs <- first StripedLogicalMergeError $ Cons.fold1M Logical.merge vss

  fromLogical s vs
{-# INLINABLE merges #-}

-- | /O(no)/
merge :: Table -> Table -> Either StripedError Table
merge x0 x1 = {-# SCC merge #-} do
  s2 <- first StripedSchemaUnionError $ Schema.union (schema x0) (schema x1)

  c0 <- toLogical =<< transmute s2 x0
  c1 <- toLogical =<< transmute s2 x1
  c2 <- first StripedLogicalMergeError $ Logical.merge c0 c1

  fromLogical s2 c2
{-# INLINABLE merge #-}

------------------------------------------------------------------------
-- Default

defaultTable :: Schema.Table -> Table
defaultTable = {-# SCC defaultTable #-} \case
  Schema.Binary def encoding ->
    Binary def encoding ""
  Schema.Array def x ->
    Array def (defaultColumn 0 x)
  Schema.Map def k v ->
    Map def (defaultColumn 0 k) (defaultColumn 0 v)
{-# INLINABLE defaultTable #-}

defaultColumn :: Int -> Schema.Column -> Column
defaultColumn n = {-# SCC defaultColumn #-} \case
  Schema.Unit ->
    Unit n
  Schema.Int def encoding ->
    Int def encoding (Generic.replicate n 0)
  Schema.Double def ->
    Double def (Generic.replicate n 0)
  Schema.Enum def vs ->
    Enum def (Generic.replicate n 0) (Cons.map (fmap (defaultColumn n)) vs)
  Schema.Struct def fs ->
    Struct def (Cons.map (fmap (defaultColumn n)) fs)
  Schema.Nested x ->
    Nested (Generic.replicate n 0) (defaultTable x)
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
transmute s t = {-# SCC transmute #-}
  case (s, t) of
    (Schema.Binary def0 encoding0, Binary def1 encoding1 bs)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $
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
transmuteColumn s c = {-# SCC transmuteColumn #-}
  case (s, c) of
    (Schema.Unit, Unit n) ->
      return $ Unit n

    (Schema.Int def0 encoding0, Int def1 encoding1 xs)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $ Int def1 encoding1 xs

    (Schema.Double def0, Double def1 xs)
      | def0 == def1
      ->
        return $ Double def1 xs

    (Schema.Enum def0 vs0, Enum def1 tags vs1)
      | def0 == def1
      , Cons.map variantName vs0 == Cons.map variantName vs1
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
transmuteStruct schemas columns0 = {-# SCC transmuteStruct #-}
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
              return . Field name $ defaultColumn n fschema

        Just column ->
          Field name <$> transmuteColumn fschema column
  in
    Cons.mapM lookupField schemas
{-# INLINABLE transmuteStruct #-}

------------------------------------------------------------------------
-- Concat/Append
--
--   These are unsafe because they don't union maps, so tables can be
--   corrupted.
--

-- | /O(sorry)/
unsafeConcat :: Cons Boxed.Vector Table -> Either StripedError Table
unsafeConcat = {-# SCC unsafeConcat #-}
  Cons.fold1M unsafeAppend
{-# INLINABLE unsafeConcat #-}

-- | /O(n)/
unsafeAppend :: Table -> Table -> Either StripedError Table
unsafeAppend x0 x1 = {-# SCC unsafeAppend #-}
  case (x0, x1) of
    (Binary def0 encoding0 bs0, Binary def1 encoding1 bs1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $ Binary def0 encoding0 (bs0 <> bs1)

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
unsafeAppendColumn x0 x1 = {-# SCC unsafeAppendColumn #-}
  case (x0, x1) of
    (Unit n0, Unit n1) ->
      return $ Unit (n0 + n1)

    (Int def0 encoding0 xs0, Int def1 encoding1 xs1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $ Int def0 encoding0 (xs0 <> xs1)

    (Double def0 xs0, Double def1 xs1)
      | def0 == def1
      ->
        return $ Double def0 (xs0 <> xs1)

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
unsafeAppendVariant v0 v1 = {-# SCC unsafeAppendVariant #-}
  if variantName v0 == variantName v1 then
    (v0 $>) <$> unsafeAppendColumn (variantData v0) (variantData v1)
  else
    Left $ StripedAppendVariantMismatch (fmap schemaColumn v0) (fmap schemaColumn v1)
{-# INLINABLE unsafeAppendVariant #-}

unsafeAppendField :: Field Column -> Field Column -> Either StripedError (Field Column)
unsafeAppendField f0 f1 = {-# SCC unsafeAppendField #-}
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
rechunk max_n = {-# SCC rechunk #-}
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

            return r

          Right (hd, tl) ->
            loop (Rechunk (n0 + length hd) emit (dl . (hd :))) tl
  in
    loop (Rechunk 0 YieldedNone id)
{-# INLINABLE rechunk #-}

unsafeFromDList :: ([Table] -> [Table]) -> [Table] -> Either StripedError Table
unsafeFromDList dl end = {-# SCC unsafeFromDList #-}
  unsafeConcat . Cons.unsafeFromList $ dl end
{-# INLINE unsafeFromDList #-}
