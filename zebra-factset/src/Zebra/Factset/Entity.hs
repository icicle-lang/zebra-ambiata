{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Factset.Entity where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)

import           Zebra.Factset.Data
import qualified Zebra.Table.Striped as Striped


data Attribute =
  Attribute {
      attributeTime :: !(Storable.Vector Time)
    , attributeFactsetId :: !(Storable.Vector FactsetId)
    , attributeTombstone :: !(Storable.Vector Tombstone)
    , attributeTable :: !Striped.Table
    } deriving (Eq, Ord, Generic)

data Entity =
  Entity {
      entityHash :: !EntityHash
    , entityId :: !EntityId
    , entityAttributes :: !(Boxed.Vector Attribute)
    } deriving (Eq, Ord, Generic)

instance Show Attribute where
  showsPrec =
    gshowsPrec

instance Show Entity where
  showsPrec =
    gshowsPrec
