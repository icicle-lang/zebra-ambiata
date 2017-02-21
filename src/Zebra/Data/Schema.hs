{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Schema (
    Schema(..)
  , Field(..)
  , FieldName(..)
  , Variant(..)
  , VariantName(..)
  , lookupVariant
  ) where

import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)


newtype FieldName =
  FieldName {
      unFieldName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show FieldName where
  showsPrec =
    gshowsPrec

data Field =
  Field {
      fieldName :: !FieldName
    , fieldSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show Field where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantName where
  showsPrec =
    gshowsPrec

data Variant =
  Variant {
      variantName :: !VariantName
    , variantSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show Variant where
  showsPrec =
    gshowsPrec

data Schema =
    Bool
  | Byte
  | Int
  | Double
  | Enum !Variant !(Boxed.Vector Variant)
  | Struct !(Boxed.Vector Field)
  | Array !Schema
    deriving (Eq, Ord, Show, Generic, Typeable)

lookupVariant :: Int -> Variant -> Boxed.Vector Variant -> Maybe Variant
lookupVariant ix variant0 variants =
  case ix of
    0 ->
      Just variant0
    _ ->
      variants Boxed.!? (ix - 1)
