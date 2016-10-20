{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block.Index (
    BlockIndex(..)
  , Tombstone(..)
  , indicesOfFacts
  ) where

import           Data.Typeable (Typeable)
import           Data.Vector.Unboxed.Deriving (derivingUnbox)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Core
import           Zebra.Data.Fact


-- FIXME Might be good if this were using 3x Storable.Vector instead of a
-- FIXME single Unboxed.Vector, as it would make translation to C smoother.
data BlockIndex =
  BlockIndex {
      indexTime :: !Time
    , indexPriority :: !Priority
    , indexTombstone :: !Tombstone
    } deriving (Eq, Ord, Show, Generic, Typeable)

indicesOfFacts :: Boxed.Vector Fact -> Unboxed.Vector BlockIndex
indicesOfFacts =
  let
    fromFact fact =
      BlockIndex
        (factTime fact)
        (factPriority fact)
        (maybe' Tombstone (const NotTombstone) $ factValue fact)
  in
    Unboxed.convert . fmap fromFact

derivingUnbox "BlockIndex"
  [t| BlockIndex -> (Time, Priority, Tombstone) |]
  [| \(BlockIndex x y z) -> (x, y, z) |]
  [| \(x, y, z) -> BlockIndex x y z |]
