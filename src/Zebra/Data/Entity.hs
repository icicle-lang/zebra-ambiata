{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Data.Entity where

import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import           Zebra.Table


data Attribute a =
  Attribute {
      attributeTime :: !(Storable.Vector Time)
    , attributeFactsetId :: !(Storable.Vector FactsetId)
    , attributeTombstone :: !(Storable.Vector Tombstone)
    , attributeTable :: !(Table a)
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

data Entity a =
  Entity {
      entityHash :: !EntityHash
    , entityId :: !EntityId
    , entityAttributes :: !(Boxed.Vector (Attribute a))
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Attribute a) where
  showsPrec =
    gshowsPrec

instance Show a => Show (Entity a) where
  showsPrec =
    gshowsPrec
