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
  , wordOfTombstone
  , tombstoneOfWord
  ) where

import           Data.Typeable (Typeable)
import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Fact


data BlockIndex =
  BlockIndex {
      indexTime :: !Time
    , indexPriority :: !Priority
    , indexTombstone :: !Tombstone
    } deriving (Eq, Ord, Show, Generic, Typeable)

data Tombstone =
    NotTombstone
  | Tombstone
    deriving (Eq, Ord, Show, Generic, Typeable)

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

wordOfTombstone :: Tombstone -> Word8
wordOfTombstone = \case
  NotTombstone ->
    0
  Tombstone ->
    1

tombstoneOfWord :: Word8 -> Tombstone
tombstoneOfWord w =
  case w of
    0 ->
      NotTombstone
    _ ->
      Tombstone

derivingUnbox "Tombstone"
  [t| Tombstone -> Word8 |]
  [| wordOfTombstone |]
  [| tombstoneOfWord |]

derivingUnbox "BlockIndex"
  [t| BlockIndex -> (Time, Priority, Tombstone) |]
  [| \(BlockIndex x y z) -> (x, y, z) |]
  [| \(x, y, z) -> BlockIndex x y z |]
