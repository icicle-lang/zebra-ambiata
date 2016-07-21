{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Entity where

import           Disorder.Jack

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data
import           Zebra.Merge.Entity


prop_entitiesOfBlock_entities :: Property
prop_entitiesOfBlock_entities =
  gamble jBlock $ \block ->
    fmap evEntity (entitiesOfBlock block) === blockEntities block


return []
tests :: IO Bool
tests =
  $quickCheckAll

