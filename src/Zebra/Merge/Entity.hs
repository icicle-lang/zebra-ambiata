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
module Zebra.Merge.Entity
  ( BlockDataId(..)
  , EntityValues(..)
  , entitiesOfBlock
  , mergeEntityValues
  , mergeEntityRecords
  , extractEntityValues
  ) where

import Zebra.Data

import qualified Data.Map as Map

import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Stream as Stream

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


-- | EntityValues is a single entity and all its values.
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
   -- ^ Indexes for current entity, indexed by attribute id
 , evRecords   :: !(Boxed.Vector (Map.Map BlockDataId Record))
   -- ^ Record values for current entity, indexed by attribute id
 }
 deriving (Eq, Generic)
instance Show EntityValues where
 showsPrec = Show.gshowsPrec


entitiesOfBlock :: BlockDataId -> Block -> Boxed.Vector EntityValues
entitiesOfBlock blockId (Block entities indices records) =
  Boxed.mapAccumulate go (indices,records) entities
  where
    go (ix,rx) ent
     = let attrs             = entityAttributes ent
           count_all         = Unboxed.sum $ Unboxed.map attributeRecords attrs
           (ix_here,ix_rest) = Unboxed.splitAt count_all ix

           dense_attrs       = denseAttributeCount rx attrs
           ix_attrs          = Generic.unsafeSplits id ix_here dense_attrs
           dense_counts      = Boxed.map Unboxed.length ix_attrs
           (rx_here,rx_rest) = Boxed.unzip $ Boxed.zipWith splitAtRecords dense_counts rx

           acc'              = (ix_rest, rx_rest)
           ix_blockId        = Boxed.map (Unboxed.map (,blockId)) ix_attrs
           rx_blockId        = Boxed.map (Map.singleton blockId) rx_here
           ev   = EntityValues ent ix_blockId rx_blockId
       in (acc', ev)



-- | Convert Attributes for a single entity into an array of counts, indexed by attribute id.
-- Attributes are sparse in attribute id, and must be sorted and unique.
-- The records is used to know how many attributes there are in total.
--
-- > denseAttributeCount
-- >    (...values for 5 attributes...)
-- >    [ Attribute (AttributeId 1) 10 , Attribute (AttributeId 3) 20 ]
-- > = [ 0, 10, 0, 20, 0 ]
--
denseAttributeCount :: Boxed.Vector Record -> Unboxed.Vector Attribute -> Unboxed.Vector Int
denseAttributeCount rs attr =
  Unboxed.mapAccumulate go attr $
  Unboxed.enumFromN 0 $ Boxed.length rs
  where
    go attrs ix
     -- Attribute ids are in the range [0..length rs-1], unique and sorted.
     -- We only need to check the head. If ids are equal, use it.
     -- If not equal, the attribute id *must* be higher than the index:
     -- otherwise we would have seen it and removed it already.
     | Just (Attribute aid acount, rest) <- Unboxed.uncons attrs
     , aid == AttributeId ix
     = (rest, acount)
     | otherwise
     = (attrs, 0)


mergeEntityValues :: Monad m => Stream.Stream m EntityValues -> Stream.Stream m EntityValues -> Stream.Stream m EntityValues
mergeEntityValues ls rs
 = Stream.merge (Stream.mergePullJoin joinEV ordEV) ls rs
 where
  -- id and hash are equal
  joinEV e1 e2
   = let evIxs = Boxed.zipWith mergeIxs (evIndices e1) (evIndices e2)
         evRcs = Boxed.zipWith mergeRcs (evRecords e1) (evRecords e2)
     in  Stream.MergePullBoth e1 { evIndices = evIxs, evRecords = evRcs }

  mergeIxs
   = Unboxed.merge (Stream.mergePullOrd (\(i,_) -> (indexTime i, indexPriority i)))
  mergeRcs
   = Map.union -- Boxed.merge (Stream.mergePullOrd fst)

  ordEV ev
   = let e = evEntity ev
     in  (entityHash e, entityId e)

-- mergeRecords: gather and concatenate all the records from different blocks.
-- This should be done after all the indices have been merged, so that it only has to
-- slice and concat the actual data once, instead of for each pair of merges.
--
-- mergeRecords :: EntityValues -> EntityValues
mergeEntityRecords :: EntityValues -> Either MergeError (Boxed.Vector Record)
mergeEntityRecords (EntityValues _ aixs recs) =
  Boxed.mapM go (Boxed.zip (Boxed.indexed aixs) recs)
  where
    go ((aid, aix), rec)
     = mergeEntityRecord (AttributeId aid) aix rec

mergeEntityRecord :: AttributeId -> Unboxed.Vector (Index, BlockDataId) -> Map.Map BlockDataId Record -> Either MergeError Record
mergeEntityRecord aid aixs records = do
  i <- init
  fst <$> Unboxed.foldM go (i, records) aixs
  where
    -- Get an 'empty' Record for this attribute.
    -- The shape of this depends on the schema of the attribute, which specifies how many fields,
    -- their types, and so on.
    -- We already have at least one non-empty record though, so we can chop it up to make an empty one.
    init =
      case Map.minView records of
        Just (r,_) ->
          return $ fst $ splitAtRecords 0 r
        Nothing ->
          Left $ MergeAttributeWithoutRecord aid

    go (build,recs) (_,blockid) = do
      (rec,recs') <- splitLookup blockid recs
      rec' <- appendRecords' build rec
      return (rec', recs')

    splitLookup blockid recs =
      case Map.lookup blockid recs of
        Just r -> do
          let (this,that) = splitAtRecords 1 r
          return (this, Map.insert blockid that recs)
        Nothing ->
          Left $ MergeBlockDataWithoutRecord aid blockid

    appendRecords' a b
     = first MergeRecordError $ appendRecords a b


extractEntityValues :: EntityValues -> Either MergeError (EntityHash, EntityId, Boxed.Vector (Unboxed.Vector Index), Boxed.Vector Record)
extractEntityValues ev@(EntityValues e aixs _) = do
  recs <- mergeEntityRecords ev
  return (entityHash e, entityId e, Boxed.map (Unboxed.map fst) aixs, recs)

