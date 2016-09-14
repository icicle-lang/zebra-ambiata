{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Merge.Entity
  ( entitiesOfBlock
  , mergeEntityValues
  , mergeEntityTables
  , entityMergedOfEntityValues
  ) where

import Zebra.Data
import Zebra.Merge.Base

import qualified Data.Map as Map

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Stream as Stream

import P



entitiesOfBlock :: Monad m => BlockDataId -> Block -> Stream.Stream m EntityValues
entitiesOfBlock blockId (Block entities indices tables) =
  Stream.streamOfVectorM $
  Boxed.mapAccumulate go (indices,tables) entities
  where
    go (ix,rx) ent
     = let attrs             = entityAttributes ent
           count_all         = Unboxed.sum $ Unboxed.map attributeRows attrs
           (ix_here,ix_rest) = Unboxed.splitAt count_all ix

           dense_attrs       = denseAttributeCount rx attrs
           ix_attrs          = Generic.unsafeSplits id ix_here dense_attrs
           dense_counts      = Boxed.map Unboxed.length ix_attrs
           (rx_here,rx_rest) = Boxed.unzip $ Boxed.zipWith splitAtTables dense_counts rx

           acc'              = (ix_rest, rx_rest)
           ix_blockId        = Boxed.map (Unboxed.map (,blockId)) ix_attrs
           rx_blockId        = Boxed.map (Map.singleton blockId) rx_here
           ev   = EntityValues ent ix_blockId rx_blockId
       in (acc', ev)



-- | Convert Attributes for a single entity into an array of counts, indexed by attribute id.
-- Attributes are sparse in attribute id, and must be sorted and unique.
-- The tables is used to know how many attributes there are in total.
--
-- > denseAttributeCount
-- >    (...values for 5 attributes...)
-- >    [ Attribute (AttributeId 1) 10 , Attribute (AttributeId 3) 20 ]
-- > = [ 0, 10, 0, 20, 0 ]
--
denseAttributeCount :: Boxed.Vector Table -> Unboxed.Vector Attribute -> Unboxed.Vector Int
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
         evRcs = Boxed.zipWith Map.union (evTables e1) (evTables e2)
     in  Stream.MergePullBoth e1 { evIndices = evIxs, evTables = evRcs }

  mergeIxs
   = Unboxed.merge (Stream.mergePullOrd (\(i,_) -> (indexTime i, indexPriority i)))

  ordEV ev
   = let e = evEntity ev
     in  (entityHash e, entityId e)

-- mergeTables: gather and concatenate all the tables from different blocks.
-- This should be done after all the indices have been merged, so that it only has to
-- slice and concat the actual data once, instead of for each pair of merges.
mergeEntityTables :: EntityValues -> Either MergeError (Boxed.Vector Table)
mergeEntityTables (EntityValues _ aixs recs) =
  Boxed.mapM go (Boxed.zip (Boxed.indexed aixs) recs)
  where
    go ((aid, aix), rec)
     = mergeEntityTable (AttributeId aid) aix rec

mergeEntityTable :: AttributeId -> Unboxed.Vector (Index, BlockDataId) -> Map.Map BlockDataId Table -> Either MergeError Table
mergeEntityTable aid aixs tables = do
  i <- init
  fst <$> Unboxed.foldM go (i, tables) aixs
  where
    -- Get an 'empty' Table for this attribute.
    -- The shape of this depends on the schema of the attribute, which specifies how many columns,
    -- their types, and so on.
    -- We already have at least one non-empty table though, so we can chop it up to make an empty one.
    init =
      case Map.minView tables of
        Just (r,_) ->
          return $ fst $ splitAtTables 0 r
        Nothing ->
          Left $ MergeAttributeWithoutTable aid

    go (build,recs) (_,blockid) = do
      (rec,recs') <- splitLookup blockid recs
      rec' <- appendTables' build rec
      return (rec', recs')

    splitLookup blockid recs =
      case Map.lookup blockid recs of
        Just r -> do
          let (this,that) = splitAtTables 1 r
          return (this, Map.insert blockid that recs)
        Nothing ->
          Left $ MergeBlockDataWithoutTable aid blockid

    appendTables' a b
     = first MergeTableError $ appendTables a b


entityMergedOfEntityValues :: EntityValues -> Either MergeError EntityMerged
entityMergedOfEntityValues ev@(EntityValues e aixs _) = do
  recs <- mergeEntityTables ev
  return $ EntityMerged (entityHash e) (entityId e) (Boxed.map (Unboxed.map fst) aixs) recs

