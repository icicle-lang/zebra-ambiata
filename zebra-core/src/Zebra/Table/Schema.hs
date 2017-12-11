{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Schema (
    Table(..)
  , Column(..)

  , SchemaError(..)
  , renderSchemaError

  , SchemaUnionError(..)
  , renderSchemaUnionError

  -- * Construction
  , bool
  , false
  , true
  , option
  , none
  , some
  , either
  , left
  , right
  , pair
  , first
  , second

  , withDefault
  , withDefaultColumn

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
  , takeOption
  , takeEither
  , takePair

  , takeDefault
  , takeDefaultColumn

  -- * Merging
  , union
  , unionColumn
  , unionStruct
  ) where

import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text

import           GHC.Generics (Generic)

import           P hiding (bool, some, either, first, second)

import           Text.Show.Pretty (ppShow)

import qualified Neutron.Vector.Boxed as Boxed
import           Neutron.Vector.Cons (Cons)
import qualified Neutron.Vector.Cons as Cons

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding


data Table =
    Binary !Default !Encoding.Binary
  | Array !Default !Column
  | Map !Default !Column !Column
    deriving (Eq, Ord, Show, Generic)

instance NFData Table

data Column =
    Unit
  | Int !Default !Encoding.Int
  | Double !Default
  | Enum !Default !(Cons Boxed.Vector (Variant Column))
  | Struct !Default !(Cons Boxed.Vector (Field Column))
  | Nested !Table
  | Reversed !Column
    deriving (Eq, Ord, Show, Generic)

instance NFData Column

data SchemaError =
    SchemaExpectedBinary !Table
  | SchemaExpectedArray !Table
  | SchemaExpectedMap !Table
  | SchemaExpectedInt !Column
  | SchemaExpectedDouble !Column
  | SchemaExpectedEnum !Column
  | SchemaExpectedStruct !Column
  | SchemaExpectedNested !Column
  | SchemaExpectedReversed !Column
  | SchemaExpectedOption !(Cons Boxed.Vector (Variant Column))
  | SchemaExpectedEither !(Cons Boxed.Vector (Variant Column))
  | SchemaExpectedPair !(Cons Boxed.Vector (Field Column))
    deriving (Eq, Show)

data SchemaUnionError =
    SchemaUnionMapKeyNotAllowed !Table !Table
  | SchemaUnionDefaultNotAllowed !(Field Column)
  | SchemaUnionFailedLookupInternalError !(Field Column)
  | SchemaUnionTableMismatch !Table !Table
  | SchemaUnionColumnMismatch !Column !Column
    deriving (Eq, Show)

renderSchemaError :: SchemaError -> Text
renderSchemaError = \case
  SchemaExpectedBinary x ->
    "Expected Binary, but was: " <> Text.pack (ppShow x)
  SchemaExpectedArray x ->
    "Expected Array, but was: " <> Text.pack (ppShow x)
  SchemaExpectedMap x ->
    "Expected Map, but was: " <> Text.pack (ppShow x)
  SchemaExpectedInt x ->
    "Expected Int, but was: " <> Text.pack (ppShow x)
  SchemaExpectedDouble x ->
    "Expected Double, but was: " <> Text.pack (ppShow x)
  SchemaExpectedEnum x ->
    "Expected Enum, but was: " <> Text.pack (ppShow x)
  SchemaExpectedStruct x ->
    "Expected Struct, but was: " <> Text.pack (ppShow x)
  SchemaExpectedNested x ->
    "Expected Nested, but was: " <> Text.pack (ppShow x)
  SchemaExpectedReversed x ->
    "Expected Reversed, but was: " <> Text.pack (ppShow x)
  SchemaExpectedOption x ->
    "Expected variants for option (i.e. none/some), but was: " <> Text.pack (ppShow x)
  SchemaExpectedEither x ->
    "Expected variants for either (i.e. left/right), but was: " <> Text.pack (ppShow x)
  SchemaExpectedPair x ->
    "Expected variants for pair (i.e. first/second), but was: " <> Text.pack (ppShow x)

renderSchemaUnionError :: SchemaUnionError -> Text
renderSchemaUnionError = \case
  SchemaUnionMapKeyNotAllowed x y ->
    "Cannot union tables with different map keys, it could invalidate the ordering invariant:" <>
    ppField "first" x <>
    ppField "second" y

  SchemaUnionDefaultNotAllowed (Field name value) ->
    "Schema did not allow defaulting of struct field:" <>
    ppField (unFieldName name) value

  SchemaUnionFailedLookupInternalError (Field name value) ->
    "This should not have happened, please report an issue. Internal error when trying to union struct field:" <>
    ppField (unFieldName name) value

  SchemaUnionTableMismatch x y ->
    "Cannot union tables with incompatible schemas:" <>
    ppField "first" x <>
    ppField "second" y

  SchemaUnionColumnMismatch x y ->
    "Cannot union columns with incompatible schemas:" <>
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

false :: Variant Column
false =
  Variant "false" Unit

true :: Variant Column
true =
  Variant "true" Unit

bool :: Default -> Column
bool def =
  Enum def $ Cons.from2 false true

none :: Variant Column
none =
  Variant "none" Unit

some :: Column -> Variant Column
some =
  Variant "some"

option :: Default -> Column -> Column
option def =
  Enum def . Cons.from2 none . some

left :: Column -> Variant Column
left =
  Variant "left"

right :: Column -> Variant Column
right =
  Variant "right"

either :: Default -> Column -> Column -> Column
either def l r =
  Enum def $ Cons.from2 (left l) (right r)

first :: Column -> Field Column
first =
  Field "first"

second :: Column -> Field Column
second =
  Field "second"

pair :: Default -> Column -> Column -> Column
pair def x y =
  Struct def $ Cons.from2 (first x) (second y)

------------------------------------------------------------------------

takeBinary :: Table -> Either SchemaError (Default, Encoding.Binary)
takeBinary = \case
  Binary def encoding ->
    Right (def, encoding)
  x ->
    Left $ SchemaExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: Table -> Either SchemaError (Default, Column)
takeArray = \case
  Array def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedArray x
{-# INLINE takeArray #-}

takeMap :: Table -> Either SchemaError (Default, Column, Column)
takeMap = \case
  Map def k v ->
    Right (def, k, v)
  x ->
    Left $ SchemaExpectedMap x
{-# INLINE takeMap #-}

takeInt :: Column -> Either SchemaError (Default, Encoding.Int)
takeInt = \case
  Int def encoding ->
    Right (def, encoding)
  x ->
    Left $ SchemaExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: Column -> Either SchemaError Default
takeDouble = \case
  Double def ->
    Right def
  x ->
    Left $ SchemaExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: Column -> Either SchemaError (Default, Cons Boxed.Vector (Variant Column))
takeEnum = \case
  Enum def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: Column -> Either SchemaError (Default, Cons Boxed.Vector (Field Column))
takeStruct = \case
  Struct def x ->
    Right (def, x)
  x ->
    Left $ SchemaExpectedStruct x
{-# INLINE takeStruct #-}

takeNested :: Column -> Either SchemaError Table
takeNested = \case
  Nested x ->
    Right x
  x ->
    Left $ SchemaExpectedNested x
{-# INLINE takeNested #-}

takeReversed :: Column -> Either SchemaError Column
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ SchemaExpectedReversed x
{-# INLINE takeReversed #-}

------------------------------------------------------------------------

takeOption :: Column -> Either SchemaError (Default, Column)
takeOption x0 = do
  (def, vs) <- takeEnum x0
  case Cons.toList vs of
    [Variant "none" Unit, Variant "some" x] ->
      return (def, x)
    _ ->
      Left $ SchemaExpectedOption vs
{-# INLINE takeOption #-}

takeEither :: Column -> Either SchemaError (Default, Column, Column)
takeEither x0 = do
  (def, vs) <- takeEnum x0
  case Cons.toList vs of
    [Variant "left" l, Variant "right" r] ->
      return (def, l, r)
    _ ->
      Left $ SchemaExpectedEither vs
{-# INLINE takeEither #-}

takePair :: Column -> Either SchemaError (Default, Column, Column)
takePair x0 = do
  (def, fs) <- takeStruct x0
  case Cons.toList fs of
    [Field "first" x, Field "second" y] ->
      return (def, x, y)
    _ ->
      Left $ SchemaExpectedPair fs
{-# INLINE takePair #-}

------------------------------------------------------------------------

takeDefault :: Table -> Default
takeDefault = \case
  Binary def _ ->
    def
  Array def _ ->
    def
  Map def _ _ ->
    def
{-# INLINABLE takeDefault #-}

takeDefaultColumn :: Column -> Default
takeDefaultColumn = \case
  Unit ->
    AllowDefault
  Int def _ ->
    def
  Double def ->
    def
  Enum def _ ->
    def
  Struct def _ ->
    def
  Nested x ->
    takeDefault x
  Reversed x ->
    takeDefaultColumn x
{-# INLINABLE takeDefaultColumn #-}

withDefault :: Default -> Table -> Table
withDefault def = \case
  Binary _ encoding ->
    Binary def encoding
  Array _ x ->
    Array def x
  Map _ k v ->
    Map def k v
{-# INLINABLE withDefault #-}

withDefaultColumn :: Default -> Column -> Column
withDefaultColumn def = \case
  Unit ->
    Unit
  Int _ encoding ->
    Int def encoding
  Double _ ->
    Double def
  Enum _ vs ->
    Enum def vs
  Struct _ fs ->
    Struct def fs
  Nested x ->
    Nested $ withDefault def x
  Reversed x ->
    Reversed $ withDefaultColumn def x
{-# INLINABLE withDefaultColumn #-}

------------------------------------------------------------------------

union :: Table -> Table -> Either SchemaUnionError Table
union t0 t1 =
  case (t0, t1) of
    (Binary def0 encoding0, Binary def1 encoding1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $ Binary def0 encoding0

    (Array def0 x0, Array def1 x1)
      | def0 == def1
      ->
        Array def0
          <$> unionColumn x0 x1

    (Map def0 k0 v0, Map def1 k1 v1)
      | k0 /= k1
      ->
        Left $ SchemaUnionMapKeyNotAllowed t0 t1

      | def0 == def1
      ->
        Map def0 k0
          <$> unionColumn v0 v1

    _ ->
      Left $ SchemaUnionTableMismatch t0 t1
{-# INLINABLE union #-}

unionColumn :: Column -> Column -> Either SchemaUnionError Column
unionColumn c0 c1 =
  case (c0, c1) of
    (Unit, Unit) ->
      return Unit

    (Int def0 encoding0, Int def1 encoding1)
      | def0 == def1
      , encoding0 == encoding1
      ->
        return $ Int def0 encoding0

    (Double def0, Double def1)
      | def0 == def1
      ->
        return $ Double def0

    (Enum def0 vs0, Enum def1 vs1)
      | def0 == def1
      , Cons.map variantName vs0 == Cons.map variantName vs1
      ->
        Enum def0
          <$> Cons.zipWithM (\(Variant n x) (Variant _ y) -> Variant n <$> unionColumn x y) vs0 vs1

    (Struct def0 fs0, Struct def1 fs1)
      | def0 == def1
      ->
        Struct def1
          <$> unionStruct fs0 fs1

    (Nested x0, Nested x1) ->
      Nested
        <$> union x0 x1

    (Reversed x0, Reversed x1) ->
      Reversed
        <$> unionColumn x0 x1

    _ ->
      Left $ SchemaUnionColumnMismatch c0 c1
{-# INLINABLE unionColumn #-}

data In a =
    One !a
  | Both !a !a

defaultOrUnion :: Map FieldName (In Column) -> Field Column -> Either SchemaUnionError (Field Column)
defaultOrUnion fields field@(Field name _) =
  case Map.lookup name fields of
    Nothing ->
      Left $ SchemaUnionFailedLookupInternalError field

    Just (One schema) ->
      case takeDefaultColumn schema of
        DenyDefault ->
          Left $ SchemaUnionDefaultNotAllowed field
        AllowDefault ->
          return $ Field name schema

    Just (Both schema0 schema1) ->
      Field name <$> unionColumn schema0 schema1

defaultOrNothing :: Map FieldName (In Column) -> Field Column -> Either SchemaUnionError (Maybe (Field Column))
defaultOrNothing fields field@(Field name _) =
  case Map.lookup name fields of
    Nothing ->
      Left $ SchemaUnionFailedLookupInternalError field

    Just (One schema) ->
      case takeDefaultColumn schema of
        DenyDefault ->
          Left $ SchemaUnionDefaultNotAllowed field
        AllowDefault ->
          return . Just $ Field name schema

    Just (Both _ _) ->
      return Nothing

fromCons :: Cons Boxed.Vector (Field Column) -> Map FieldName Column
fromCons =
  Map.fromList .
  fmap (\(Field k v) -> (k, v)) .
  Cons.toList

unionStruct ::
     Cons Boxed.Vector (Field Column)
  -> Cons Boxed.Vector (Field Column)
  -> Either SchemaUnionError (Cons Boxed.Vector (Field Column))
unionStruct cfields0 cfields1 = do
  let
    fields =
      Map.mergeWithKey
        (\_ x y -> Just (Both x y))
        (fmap One)
        (fmap One)
        (fromCons cfields0)
        (fromCons cfields1)

  xs <- traverse (defaultOrUnion fields) cfields0
  ys <- traverse (defaultOrNothing fields) cfields1

  return . Cons.unsafeFromVector $
    Cons.toVector xs <>
    Cons.mapMaybe id ys
{-# INLINABLE unionStruct #-}
