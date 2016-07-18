{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block (
    Block(..)
  , blockOfFacts
  ) where

import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Encoding
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Data.Index
import           Zebra.Data.Record


data Block =
  Block {
      blockEpoch :: !Time
    , blockEntities :: !(Boxed.Vector Entity)
    , blockIndices :: !(Unboxed.Vector Index)
    , blockRecords :: !(Boxed.Vector Record)
    } deriving (Eq, Ord, Show, Generic, Typeable)

blockOfFacts :: Boxed.Vector Encoding -> Time -> Boxed.Vector Fact -> Either RecordError Block
blockOfFacts encodings epoch facts =
  Block epoch (entitiesOfFacts facts) (indicesOfFacts facts)
    <$> recordsOfFacts encodings facts
