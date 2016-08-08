{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Merge.Base
  ( BlockDataId(..)
  , EntityMerged(..)
  , EntityValues(..)
  , MergeError(..)
  , treeFold
  ) where

import Zebra.Data

import qualified Data.Map as Map

import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import qualified X.Text.Show as Show
import GHC.Generics (Generic(..))

import P

-- | A BlockDataId roughly corresponds to the id of the file, where a Record came from.
newtype BlockDataId
 = BlockDataId
 { unDataChunkId :: Int
 } deriving (Eq, Ord, Generic)
instance Show BlockDataId where
 showsPrec = Show.gshowsPrec

derivingUnbox "BlockDataId"
  [t| BlockDataId -> Int |]
  [| \(BlockDataId x) -> x |]
  [| \x -> BlockDataId x |]

data MergeError =
    MergeRecordError !RecordError
  | MergeAttributeWithoutRecord !AttributeId
  | MergeBlockDataWithoutRecord !AttributeId !BlockDataId
    deriving (Eq, Show)


-- | EntityMerged is an entity with all its values, after merging has finished.
-- These are the real values of the entity.
data EntityMerged
 = EntityMerged
 { emEntityHash :: !EntityHash
 , emEntityId   :: !EntityId
 , emIndices    :: !(Boxed.Vector (Unboxed.Vector Index))
   -- ^ Indices for current entity, indexed by attribute id
 , emRecords    :: !(Boxed.Vector Record)
   -- ^ Record values for current entity, indexed by attribute id
 }
 deriving (Eq, Generic)
instance Show EntityMerged where
 showsPrec = Show.gshowsPrec



-- | EntityValues is an intermediate type used during merging.
-- It contains a single entity and all its values.
-- It has multiple chunks of Records, one for each input file and attribute.
-- When merging entities from many files we don't want to chop the data up
-- and cat it back together multiple times.
-- So it's better to just store the raw, unchopped records from each input file,
-- and each index also stores which file it came from.
-- Then after all files have been merged, we can chop all input files once, merge them
-- according to the indices, then concatenate.
--
-- Invariants: 
--   * length evIndicies == length evRecords
--      (evIndices and evRecords are same length)
--   * forall i, length (evRecords ! i) > 0
--      (each attribute has at least one Record)
--   * forall i j, snd (evIndicies ! i ! j) `Map.member` evRecords ! i
--      (each BlockDataId in evIndices refers to a BlockDataId in evRecords)
--      (the converse isn't true: there can be Records that have no indices)
--
data EntityValues
 = EntityValues
 { evEntity    :: !Entity
 , evIndices   :: !(Boxed.Vector (Unboxed.Vector (Index, BlockDataId)))
   -- ^ Indices for current entity, indexed by attribute id
 , evRecords   :: !(Boxed.Vector (Map.Map BlockDataId Record))
   -- ^ Record values for current entity, indexed by attribute id
 }
 deriving (Eq, Generic)
instance Show EntityValues where
 showsPrec = Show.gshowsPrec



-- | Perform a divide and conquer tree fold on a vector.
-- The 'kons' operation needs to be associative, because it is applied to
-- recursive halves of the vector.
--
-- This is used for merging streams together, and should require fewer comparisons.
treeFold  :: (a -> a -> a)
          -> a
          -> (c -> a)
          -> Boxed.Vector c
          -> a
treeFold k z f vec
 = go vec
 where
  go vs
   | Boxed.length vs == 0
   = z
   | Boxed.length vs == 1
   = f $ vs Boxed.! 0
   | otherwise
   = let (a,b) = Boxed.splitAt (Boxed.length vs `div` 2) vs
     in  go a `k` go b
{-# INLINE treeFold #-} 

