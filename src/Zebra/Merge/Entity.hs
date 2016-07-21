{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Merge.Entity
  ( EntityValues(..)
  , entitiesOfBlock
  , recordSplitAt
  ) where

import Zebra.Data

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Storable as Storable
import qualified Data.ByteString as B

import qualified X.Text.Show as Show
import GHC.Generics (Generic(..))

import P

data EntityValues
 = EntityValues
 { evEntity    :: !Entity
 , evIndices   :: !(Boxed.Vector (Unboxed.Vector Index))
   -- ^ Indexes for current entity, indexed by attribute id
 , evRecords   :: !(Boxed.Vector Record)
   -- ^ Record values for current entity, indexed by attribute id
 }
 deriving (Eq, Generic)
instance Show EntityValues where
 showsPrec = Show.gshowsPrec


entitiesOfBlock :: Block -> Boxed.Vector EntityValues
entitiesOfBlock (Block entities indices records) =
  Boxed.mapAccumulate go (indices,records) entities
  where
    go (ix,rx) ent
     = let attrs             = entityAttributes ent
           count_all         = Unboxed.sum $ Unboxed.map attributeRecords attrs
           (ix_here,ix_rest) = Unboxed.splitAt count_all ix

           dense_attrs       = denseAttributeCount rx attrs
           ix_attrs          = Generic.unsafeSplits id ix_here dense_attrs
           dense_counts      = Boxed.map countNotTombstone ix_attrs
           (rx_here,rx_rest) = Boxed.unzip $ Boxed.zipWith recordSplitAt dense_counts rx

           acc' = (ix_rest, rx_rest)
           ev   = EntityValues ent ix_attrs rx_here
       in (acc', ev)


countNotTombstone :: Unboxed.Vector Index -> Int
countNotTombstone ixs =
  Unboxed.length $ Unboxed.filter ((==NotTombstone) . indexTombstone) ixs


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


recordSplitAt :: Int -> Record -> (Record, Record)
recordSplitAt i (Record fs) =
  let (as,bs) = Boxed.unzip $ Boxed.map (fieldSplitAt i) fs
  in  (Record as, Record bs)

fieldSplitAt :: Int -> Field -> (Field, Field)
fieldSplitAt i =
  \case
    ByteField vs
     -> bye ByteField $ B.splitAt i vs
    WordField vs
     -> bye WordField $ Storable.splitAt i vs
    DoubleField vs
     -> bye DoubleField $ Storable.splitAt i vs
    ListField len rec
     -> let (len1, len2) = Storable.splitAt i len
            nested_count = fromIntegral $ Storable.sum len1
            (rec1, rec2) = recordSplitAt nested_count rec
        in  (ListField len1 rec1, ListField len2 rec2)
  where
   bye f = bimap f f

