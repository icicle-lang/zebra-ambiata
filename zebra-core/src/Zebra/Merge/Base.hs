{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
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
  , renderMergeError
  , treeFold
  ) where

import qualified Data.Map as Map
import qualified Data.Text as Text
import           Data.Vector.Unboxed.Deriving (derivingUnbox)

import           GHC.Generics (Generic(..))

import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Text.Show as Show

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Table (Table, TableError)


-- | A BlockDataId roughly corresponds to the id of the file, where a Table came from.
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
    MergeTableError !TableError
  | MergeAttributeWithoutTable !AttributeId
  | MergeBlockDataWithoutTable !AttributeId !BlockDataId
    deriving (Eq, Show)

-- This could certainly be nicer
renderMergeError :: MergeError -> Text
renderMergeError = \case
  MergeTableError r ->
    -- Wouldn't hurt to add a renderTableError
    "Merge error when appending tables. This might mean the files have different schemas or one of the files is invalid.\n" <>
    "The error was: " <> Text.pack (show r)
  MergeAttributeWithoutTable attr ->
    "Merge error: attribute " <> Text.pack (show attr) <> " has no values, but is expected to have values. This could be an invalid input file."
  MergeBlockDataWithoutTable attr blockid ->
    "Merge error: the block " <> Text.pack (show blockid) <> " refers to attribute " <> Text.pack (show attr) <> ", but there are no values for this. This could be an invalid input file."

-- | EntityMerged is an entity with all its values, after merging has finished.
-- These are the real values of the entity.
data EntityMerged
 = EntityMerged
 { emEntityHash :: !EntityHash
 , emEntityId   :: !EntityId
 , emIndices    :: !(Boxed.Vector (Unboxed.Vector BlockIndex))
   -- ^ Indices for current entity, indexed by attribute id
 , emTables    :: !(Boxed.Vector Table)
   -- ^ Table values for current entity, indexed by attribute id
 }
 deriving (Eq, Generic)
instance Show EntityMerged where
 showsPrec = Show.gshowsPrec



-- | EntityValues is an intermediate type used during merging.
-- It contains a single entity and all its values.
-- It has multiple chunks of Tables, one for each input file and attribute.
-- When merging entities from many files we don't want to chop the data up
-- and cat it back together multiple times.
-- So it's better to just store the raw, unchopped tables from each input file,
-- and each index also stores which file it came from.
-- Then after all files have been merged, we can chop all input files once, merge them
-- according to the indices, then concatenate.
--
-- Invariants:
--   * length evIndicies == length evTables
--      (evIndices and evTables are same length)
--   * forall i, length (evTables ! i) > 0
--      (each attribute has at least one Table)
--   * forall i j, snd (evIndicies ! i ! j) `Map.member` evTables ! i
--      (each BlockDataId in evIndices refers to a BlockDataId in evTables)
--      (the converse isn't true: there can be Tables that have no indices)
--
data EntityValues
 = EntityValues
 { evEntity    :: !BlockEntity
 , evIndices   :: !(Boxed.Vector (Unboxed.Vector (BlockIndex, BlockDataId)))
   -- ^ Indices for current entity, indexed by attribute id
 , evTables   :: !(Boxed.Vector (Map.Map BlockDataId Table))
   -- ^ Table values for current entity, indexed by attribute id
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

