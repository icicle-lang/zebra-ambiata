{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Entity (
    Entity(..)
  , Attribute(..)
  , entitiesOfFacts
  ) where

import           Data.Typeable (Typeable)
import           Data.Vector.Unboxed.Deriving (derivingUnbox)

import           GHC.Generics (Generic)

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Data.Fact


data Attribute =
  Attribute {
      attributeId :: !AttributeId
    , attributeRows :: !Int
    } deriving (Eq, Ord, Show, Generic, Typeable)

-- This deriving Unbox needs to appear before using it in Entity below
derivingUnbox "Attribute"
  [t| Attribute -> (AttributeId, Int) |]
  [| \(Attribute x y) -> (x, y) |]
  [| \(x, y) -> Attribute x y |]


data Entity =
  Entity {
      entityHash :: !EntityHash
    , entityId :: !EntityId
    , entityAttributes :: !(Unboxed.Vector Attribute)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data EntityAcc =
  EntityAcc !EntityHash !EntityId !AttributeAcc !(Boxed.Vector Entity)

data AttributeAcc =
  AttributeAcc !AttributeId !Int !(Unboxed.Vector Attribute)

-- | Convert facts to hierarchical entity representation.
--
--   The input fact vector must be sorted by:
--
--     1. Entity Hash
--     2. Entity Id
--     3. Attribute
--     4. Time
--     5. Priority
--
entitiesOfFacts :: Boxed.Vector Fact -> Boxed.Vector Entity
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

takeEntities :: EntityAcc -> Boxed.Vector Entity
takeEntities (EntityAcc ehash eid attrs ents) =
  ents `Boxed.snoc` Entity ehash eid (takeAttributes attrs)

takeAttributes :: AttributeAcc -> Unboxed.Vector Attribute
takeAttributes (AttributeAcc aid nrecs attrs) =
  attrs `Unboxed.snoc` Attribute aid nrecs

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
