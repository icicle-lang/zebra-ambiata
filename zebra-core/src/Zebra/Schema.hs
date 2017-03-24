{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Schema (
    TableSchema(..)
  , ColumnSchema(..)
  , Field(..)
  , FieldName(..)
  , Variant(..)
  , VariantName(..)
  , Tag(..)

  , hasVariant
  , forVariant
  , lookupVariant

  , bool
  , false
  , true
  , option
  , none
  , some

  , SchemaError(..)
  , renderSchemaError

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

  , foreignOfTags
  , tagsOfForeign
  ) where

import           Data.String (IsString(..))
import qualified Data.Text as Text
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           Foreign.Storable (Storable)

import           P hiding (bool, some)

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons


newtype FieldName =
  FieldName {
      unFieldName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show FieldName where
  showsPrec p =
    showsPrec p . unFieldName

instance IsString FieldName where
  fromString =
    FieldName . Text.pack

data Field a =
  Field {
      fieldName :: !FieldName
    , field :: !a
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Field a) where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantName where
  showsPrec p =
    showsPrec p . unVariantName

instance IsString VariantName where
  fromString =
    VariantName . Text.pack

data Variant a =
  Variant {
      variantName :: !VariantName
    , variant :: !a
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Variant a) where
  showsPrec =
    gshowsPrec

newtype Tag =
  Tag {
      unTag :: Int64
    } deriving (Eq, Ord, Generic, Typeable, Storable, Num, Enum, Real, Integral)

instance Show Tag where
  showsPrec =
    gshowsPrec

data TableSchema =
    Binary
  | Array !ColumnSchema
  | Map !ColumnSchema !ColumnSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

data ColumnSchema =
    Unit
  | Int
  | Double
  | Enum !(Cons Boxed.Vector (Variant ColumnSchema))
  | Struct !(Cons Boxed.Vector (Field ColumnSchema))
  | Nested !TableSchema
  | Reversed !ColumnSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

data SchemaError =
    SchemaExpectedBinary !TableSchema
  | SchemaExpectedArray !TableSchema
  | SchemaExpectedMap !TableSchema
  | SchemaExpectedInt !ColumnSchema
  | SchemaExpectedDouble !ColumnSchema
  | SchemaExpectedEnum !ColumnSchema
  | SchemaExpectedStruct !ColumnSchema
  | SchemaExpectedNested !ColumnSchema
  | SchemaExpectedReversed !ColumnSchema
  -- FIXME Should non-primitive types really be here? /not sure
  | SchemaExpectedOption !(Cons Boxed.Vector (Variant ColumnSchema))
    deriving (Eq, Ord, Show, Generic, Typeable)

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

hasVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Bool
hasVariant tag xs =
  fromIntegral tag < Cons.length xs
{-# INLINE hasVariant #-}

forVariant ::
  Monad m =>
  Cons Boxed.Vector (Variant a) ->
  (Tag -> VariantName -> a -> m b) ->
  m (Cons Boxed.Vector (Variant b))
forVariant xs f =
  Cons.iforM xs $ \i (Variant name x) ->
    Variant name <$> f (fromIntegral i) name x

lookupVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Maybe (Variant a)
lookupVariant tag xs =
  Cons.index (fromIntegral tag) xs
{-# INLINE lookupVariant #-}

------------------------------------------------------------------------

false :: Variant ColumnSchema
false =
  Variant (VariantName "false") Unit

true :: Variant ColumnSchema
true =
  Variant (VariantName "true") Unit

bool :: ColumnSchema
bool =
  Enum $ Cons.from2 false true

none :: Variant ColumnSchema
none =
  Variant (VariantName "none") Unit

some :: ColumnSchema -> Variant ColumnSchema
some =
  Variant (VariantName "some")

option :: ColumnSchema -> ColumnSchema
option =
  Enum . Cons.from2 none . some

------------------------------------------------------------------------

takeBinary :: TableSchema -> Either SchemaError ()
takeBinary = \case
  Binary ->
    Right ()
  x ->
    Left $ SchemaExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: TableSchema -> Either SchemaError ColumnSchema
takeArray = \case
  Array x ->
    Right x
  x ->
    Left $ SchemaExpectedArray x
{-# INLINE takeArray #-}

takeMap :: TableSchema -> Either SchemaError (ColumnSchema, ColumnSchema)
takeMap = \case
  Map k v ->
    Right (k, v)
  x ->
    Left $ SchemaExpectedMap x
{-# INLINE takeMap #-}

takeInt :: ColumnSchema -> Either SchemaError ()
takeInt = \case
  Int ->
    Right ()
  x ->
    Left $ SchemaExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: ColumnSchema -> Either SchemaError ()
takeDouble = \case
  Double ->
    Right ()
  x ->
    Left $ SchemaExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: ColumnSchema -> Either SchemaError (Cons Boxed.Vector (Variant ColumnSchema))
takeEnum = \case
  Enum x ->
    Right x
  x ->
    Left $ SchemaExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: ColumnSchema -> Either SchemaError (Cons Boxed.Vector (Field ColumnSchema))
takeStruct = \case
  Struct x ->
    Right x
  x ->
    Left $ SchemaExpectedStruct x
{-# INLINE takeStruct #-}

takeNested :: ColumnSchema -> Either SchemaError TableSchema
takeNested = \case
  Nested x ->
    Right x
  x ->
    Left $ SchemaExpectedNested x
{-# INLINE takeNested #-}

takeReversed :: ColumnSchema -> Either SchemaError ColumnSchema
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ SchemaExpectedReversed x
{-# INLINE takeReversed #-}

------------------------------------------------------------------------

takeOption :: ColumnSchema -> Either SchemaError ColumnSchema
takeOption x0 = do
  vs <- takeEnum x0
  case Cons.toList vs of
    [Variant "none" Unit, Variant "some" x] ->
      pure x
    _ ->
      Left $ SchemaExpectedOption vs
{-# INLINE takeOption #-}

------------------------------------------------------------------------

foreignOfTags :: Storable.Vector Tag -> Storable.Vector Int64
foreignOfTags =
  Storable.unsafeCast
{-# INLINE foreignOfTags #-}

tagsOfForeign :: Storable.Vector Int64 -> Storable.Vector Tag
tagsOfForeign =
  Storable.unsafeCast
{-# INLINE tagsOfForeign #-}
