{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Encoding (
    Encoding(..)
  , FieldName(..)
  , FieldEncoding(..)
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

data FieldEncoding =
  FieldEncoding {
      fieldObligation :: !FieldObligation
    , fieldEncoding :: !Encoding
    } deriving (Eq, Ord, Show, Generic, Typeable)

data Encoding =
    BoolEncoding
  | Int64Encoding
  | DoubleEncoding
  | StringEncoding
  | DateEncoding
  | StructEncoding !(Boxed.Vector (FieldName, FieldEncoding))
  | ListEncoding !Encoding
    deriving (Eq, Ord, Show, Generic, Typeable)
