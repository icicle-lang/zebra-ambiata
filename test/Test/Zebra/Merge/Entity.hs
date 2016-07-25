{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
module Test.Zebra.Merge.Entity where

import           Disorder.Jack

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data
import           Zebra.Merge.Entity

import qualified X.Data.Vector as Boxed

fakeBlockId :: BlockDataId
fakeBlockId = BlockDataId 0


prop_entitiesOfBlock_entities :: Property
prop_entitiesOfBlock_entities =
  gamble jBlock $ \block ->
    fmap evEntity (entitiesOfBlock fakeBlockId block) === blockEntities block

prop_entitiesOfBlock_indices :: Property
prop_entitiesOfBlock_indices =
  gamble jBlockValid $ \block ->
    catIndices (entitiesOfBlock fakeBlockId block) === takeIndices block
 where
  catIndices evs
   = Boxed.map fst
   $ Boxed.concatMap Boxed.convert
   $ Boxed.concatMap evIndices evs

  takeIndices block
   = Boxed.convert
   $ blockIndices block


return []
tests :: IO Bool
tests =
  $quickCheckAll

