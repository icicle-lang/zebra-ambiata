{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Schema (
    Table(..)
  , Column(..)

  , SchemaError(..)
  , renderSchemaError

  , bool
  , false
  , true
  , option
  , none
  , some

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

  , takeDefault
  , takeDefaultColumn
  , withDefault
  , withDefaultColumn
  ) where

import qualified Data.Text as Text

import           GHC.Generics (Generic)

import           P hiding (bool, some)

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding


data Table =
    Binary !Default !(Maybe Encoding.Binary)
  | Array !Default !Column
  | Map !Default !Column !Column
    deriving (Eq, Ord, Show, Generic)

data Column =
    Unit
  | Int !Default
  | Double !Default
  | Enum !Default !(Cons Boxed.Vector (Variant Column))
  | Struct !Default !(Cons Boxed.Vector (Field Column))
  | Nested !Table
  | Reversed !Column
    deriving (Eq, Ord, Show, Generic)

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
  -- FIXME Should non-primitive types really be here? /not sure
  | SchemaExpectedOption !(Cons Boxed.Vector (Variant Column))
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
    "Expected option variants (i.e. none/some), but was: " <> Text.pack (ppShow x)

------------------------------------------------------------------------

false :: Variant Column
false =
  Variant (VariantName "false") Unit

true :: Variant Column
true =
  Variant (VariantName "true") Unit

bool :: Default -> Column
bool def =
  Enum def $ Cons.from2 false true

none :: Variant Column
none =
  Variant (VariantName "none") Unit

some :: Column -> Variant Column
some =
  Variant (VariantName "some")

option :: Default -> Column -> Column
option def =
  Enum def . Cons.from2 none . some

------------------------------------------------------------------------

takeBinary :: Table -> Either SchemaError (Default, Maybe Encoding.Binary)
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

takeInt :: Column -> Either SchemaError Default
takeInt = \case
  Int def ->
    Right def
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
      pure (def, x)
    _ ->
      Left $ SchemaExpectedOption vs
{-# INLINE takeOption #-}

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
  Int def ->
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
  Int _ ->
    Int def
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
