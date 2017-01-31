{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Data.Entity where

import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import           Zebra.Data.Table


data Attribute =
  Attribute {
      attributeTime :: !(Storable.Vector Time)
    , attributeFactsetId :: !(Storable.Vector FactsetId)
    , attributeTombstone :: !(Storable.Vector Tombstone)
    , attributeTable :: !Table
    } deriving (Eq, Ord, Generic, Typeable)

data Entity =
  Entity {
      entityHash :: !EntityHash
    , entityId :: !EntityId
    , entityAttributes :: !(Boxed.Vector Attribute)
    } deriving (Eq, Ord, Generic, Typeable)

instance Show Attribute where
  showsPrec =
    gshowsPrec

instance Show Entity where
  showsPrec =
    gshowsPrec
