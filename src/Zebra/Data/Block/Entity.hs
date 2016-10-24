{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Block.Entity (
    BlockEntity(..)
  , BlockAttribute(..)
  , entitiesOfFacts
  ) where

import           Data.Typeable (Typeable)
import           Data.Vector.Unboxed.Deriving (derivingUnbox)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Core
import           Zebra.Data.Fact


data BlockAttribute =
  BlockAttribute {
      attributeId :: !AttributeId
    , attributeRows :: !Int64
    } deriving (Eq, Ord, Show, Generic, Typeable)

-- This deriving Unbox needs to appear before using it in BlockEntity below
derivingUnbox "BlockAttribute"
  [t| BlockAttribute -> (AttributeId, Int64) |]
  [| \(BlockAttribute x y) -> (x, y) |]
  [| \(x, y) -> BlockAttribute x y |]

data BlockEntity =
  BlockEntity {
      entityHash :: !EntityHash
    , entityId :: !EntityId
    , entityAttributes :: !(Unboxed.Vector BlockAttribute)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data EntityAcc =
  EntityAcc !EntityHash !EntityId !AttributeAcc !(Boxed.Vector BlockEntity)

data AttributeAcc =
  AttributeAcc !AttributeId !Int64 !(Unboxed.Vector BlockAttribute)

-- | Convert facts to hierarchical BlockEntity representation.
--
--   The input fact vector must be sorted by:
--
--     1. BlockEntity Hash
--     2. BlockEntity Id
--     3. BlockAttribute
--     4. Time
--     5. Priority
--
entitiesOfFacts :: Boxed.Vector Fact -> Boxed.Vector BlockEntity
entitiesOfFacts =
  let
    loop macc fact =
      case macc of
        Nothing' ->
          Just' $! mkEntityAcc fact
        Just' acc ->
          Just' $! appendEntity acc fact
  in
    maybe' Boxed.empty takeEntities . Boxed.foldl' loop Nothing'

mkAttributeAcc :: AttributeId -> AttributeAcc
mkAttributeAcc aid =
  AttributeAcc aid 1 Unboxed.empty

mkEntityAcc :: Fact -> EntityAcc
mkEntityAcc (Fact ehash eid aid _ _ _) =
  EntityAcc ehash eid (mkAttributeAcc aid) Boxed.empty

takeEntities :: EntityAcc -> Boxed.Vector BlockEntity
takeEntities (EntityAcc ehash eid attrs ents) =
  ents `Boxed.snoc` BlockEntity ehash eid (takeAttributes attrs)

takeAttributes :: AttributeAcc -> Unboxed.Vector BlockAttribute
takeAttributes (AttributeAcc aid nrecs attrs) =
  attrs `Unboxed.snoc` BlockAttribute aid nrecs

appendEntity :: EntityAcc -> Fact -> EntityAcc
appendEntity acc0@(EntityAcc ehash0 eid0 attrs0 ents0) (Fact ehash1 eid1 aid1 _ _ _) =
  if ehash0 == ehash1 && eid0 == eid1 then
    EntityAcc ehash0 eid0 (appendAttribute attrs0 aid1) ents0
  else
    EntityAcc ehash1 eid1 (mkAttributeAcc aid1) (takeEntities acc0)

appendAttribute :: AttributeAcc -> AttributeId -> AttributeAcc
appendAttribute acc0@(AttributeAcc aid0 recs0 attrs0) aid1 =
  if aid0 == aid1 then
    AttributeAcc aid0 (recs0 + 1) attrs0
  else
    AttributeAcc aid1 1 (takeAttributes acc0)
