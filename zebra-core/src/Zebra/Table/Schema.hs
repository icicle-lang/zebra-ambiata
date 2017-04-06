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
  ) where

import qualified Data.Text as Text

import           GHC.Generics (Generic)

import           P hiding (bool, some)

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data


data Table =
    Binary
  | Array !Column
  | Map !Column !Column
    deriving (Eq, Ord, Show, Generic)

data Column =
    Unit
  | Int
  | Double
  | Enum !(Cons Boxed.Vector (Variant Column))
  | Struct !(Cons Boxed.Vector (Field Column))
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

bool :: Column
bool =
  Enum $ Cons.from2 false true

none :: Variant Column
none =
  Variant (VariantName "none") Unit

some :: Column -> Variant Column
some =
  Variant (VariantName "some")

option :: Column -> Column
option =
  Enum . Cons.from2 none . some

------------------------------------------------------------------------

takeBinary :: Table -> Either SchemaError ()
takeBinary = \case
  Binary ->
    Right ()
  x ->
    Left $ SchemaExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: Table -> Either SchemaError Column
takeArray = \case
  Array x ->
    Right x
  x ->
    Left $ SchemaExpectedArray x
{-# INLINE takeArray #-}

takeMap :: Table -> Either SchemaError (Column, Column)
takeMap = \case
  Map k v ->
    Right (k, v)
  x ->
    Left $ SchemaExpectedMap x
{-# INLINE takeMap #-}

takeInt :: Column -> Either SchemaError ()
takeInt = \case
  Int ->
    Right ()
  x ->
    Left $ SchemaExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: Column -> Either SchemaError ()
takeDouble = \case
  Double ->
    Right ()
  x ->
    Left $ SchemaExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: Column -> Either SchemaError (Cons Boxed.Vector (Variant Column))
takeEnum = \case
  Enum x ->
    Right x
  x ->
    Left $ SchemaExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: Column -> Either SchemaError (Cons Boxed.Vector (Field Column))
takeStruct = \case
  Struct x ->
    Right x
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

takeOption :: Column -> Either SchemaError Column
takeOption x0 = do
  vs <- takeEnum x0
  case Cons.toList vs of
    [Variant "none" Unit, Variant "some" x] ->
      pure x
    _ ->
      Left $ SchemaExpectedOption vs
{-# INLINE takeOption #-}
