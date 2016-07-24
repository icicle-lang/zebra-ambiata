{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Data.Record where

import           Disorder.Jack (Property, quickCheckAll)
import           Disorder.Jack (gamble, tripping, arbitrary)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Data.Record


prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt =
  gamble arbitrary $ \ix ->
  gamble jRecord $
    tripping (splitAtRecords ix) (uncurry appendRecords)

return []
tests :: IO Bool
tests =
  $quickCheckAll
