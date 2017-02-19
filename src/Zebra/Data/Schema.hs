{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Schema (
    Schema(..)
  , FieldSchema(..)
  , FieldName(..)
  , VariantSchema(..)
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

data FieldSchema =
  FieldSchema {
      fieldName :: !FieldName
    , fieldSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show FieldSchema where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantName where
  showsPrec =
    gshowsPrec

data VariantSchema =
  VariantSchema {
      variantName :: !VariantName
    , variantSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantSchema where
  showsPrec =
    gshowsPrec

data Schema =
    BoolSchema
  | Int64Schema
  | DoubleSchema
  | StringSchema
  | DateSchema
  | ListSchema !Schema
  | StructSchema !(Boxed.Vector FieldSchema)
  | EnumSchema !VariantSchema !(Boxed.Vector VariantSchema)
    deriving (Eq, Ord, Show, Generic, Typeable)

lookupVariant :: Int -> VariantSchema -> Boxed.Vector VariantSchema -> Maybe VariantSchema
lookupVariant ix variant0 variants =
  case ix of
    0 ->
      Just variant0
    _ ->
      variants Boxed.!? (ix + 1)
