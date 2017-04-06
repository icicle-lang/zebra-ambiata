{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Data (
    Field(..)
  , FieldName(..)
  , Variant(..)
  , VariantName(..)
  , Tag(..)

  , hasVariant
  , lookupVariant
  , forVariant

  , foreignOfTags
  , tagsOfForeign
  ) where

import           Data.String (IsString(..))
import qualified Data.Text as Text

import           Foreign.Storable (Storable)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons

newtype FieldName =
  FieldName {
      unFieldName :: Text
    } deriving (Eq, Ord, Generic)

instance Show FieldName where
  showsPrec p =
    showsPrec p . unFieldName

instance IsString FieldName where
  fromString =
    FieldName . Text.pack

data Field a =
  Field {
      fieldName :: !FieldName
    , fieldData :: !a
    } deriving (Eq, Ord, Generic, Functor, Foldable, Traversable)

instance Show a => Show (Field a) where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic)

instance Show VariantName where
  showsPrec p =
    showsPrec p . unVariantName

instance IsString VariantName where
  fromString =
    VariantName . Text.pack

data Variant a =
  Variant {
      variantName :: !VariantName
    , variantData :: !a
    } deriving (Eq, Ord, Generic, Functor, Foldable, Traversable)

instance Show a => Show (Variant a) where
  showsPrec =
    gshowsPrec

newtype Tag =
  Tag {
      unTag :: Int64
    } deriving (Eq, Ord, Generic, Storable, Num, Enum, Real, Integral)

instance Show Tag where
  showsPrec =
    gshowsPrec

------------------------------------------------------------------------

hasVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Bool
hasVariant tag xs =
  fromIntegral tag < Cons.length xs
{-# INLINE hasVariant #-}

lookupVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Maybe (Variant a)
lookupVariant tag xs =
  Cons.index (fromIntegral tag) xs
{-# INLINE lookupVariant #-}

forVariant ::
  Monad m =>
  Cons Boxed.Vector (Variant a) ->
  (Tag -> VariantName -> a -> m b) ->
  m (Cons Boxed.Vector (Variant b))
forVariant xs f =
  Cons.iforM xs $ \i (Variant name x) ->
    Variant name <$> f (fromIntegral i) name x

------------------------------------------------------------------------

foreignOfTags :: Storable.Vector Tag -> Storable.Vector Int64
foreignOfTags =
  Storable.unsafeCast
{-# INLINE foreignOfTags #-}

tagsOfForeign :: Storable.Vector Int64 -> Storable.Vector Tag
tagsOfForeign =
  Storable.unsafeCast
{-# INLINE tagsOfForeign #-}
