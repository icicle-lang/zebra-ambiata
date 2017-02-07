{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Schema (
    Schema(..)
  , FieldName(..)
  , FieldSchema(..)
  , FieldObligation(..)
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

data FieldObligation =
    OptionalField
  | RequiredField
    deriving (Eq, Ord, Read, Show, Generic, Typeable)

data FieldSchema =
  FieldSchema {
      fieldObligation :: !FieldObligation
    , fieldSchema :: !Schema
    } deriving (Eq, Ord, Show, Generic, Typeable)

data Schema =
    BoolSchema
  | Int64Schema
  | DoubleSchema
  | StringSchema
  | DateSchema
  | StructSchema !(Boxed.Vector (FieldName, FieldSchema))
  | ListSchema !Schema
    deriving (Eq, Ord, Show, Generic, Typeable)
